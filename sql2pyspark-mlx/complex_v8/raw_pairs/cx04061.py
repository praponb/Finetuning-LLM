from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_month_by_risk_scor = Window.partitionBy(F.month("joined_on")).orderBy("risk_score", "id")
w_region_joined_on_year = Window.partitionBy("region", F.year("joined_on"))
w_joined_on_month_by_risk_scor_2 = Window.partitionBy(F.month("joined_on")).orderBy("risk_score", "id").rowsBetween(-1, -1)

prepared_holders = (
    df_policyholders
    .filter((F.col("region").isNotNull()) & (F.col("joined_on") < F.lit("2025-02-22").cast("date")))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        F.round(F.col("risk_score") * 1.07, 2).alias("risk_score_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("segment")), " ", "_")).alias("segment_slug"),
        F.concat(F.col("region"), F.lit("#"), F.col("id").cast("string")).alias("region_ref"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_policy_premium"))
)

with_stats = (
    prepared_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.max_policy_premium")
)

tagged = (
    with_stats
    .filter(F.col("region") == "East")
    .withColumn("region_or_unknown", F.coalesce(F.col("region"), F.lit("UNKNOWN")))
)

sequenced = (
    tagged
    .select(
        "id",
        "risk_score",
        "joined_on",
        "risk_score_adj",
        "region_ref",
        "max_policy_premium",
        "region_or_unknown",
        F.lag("max_policy_premium", 3).over(w_joined_on_month_by_risk_scor).alias("prev3_max_policy_premium"),
        F.max("risk_score_adj").over(w_region_joined_on_year).alias("region_joined_on_year_max_risk_score_adj"),
        F.sum("max_policy_premium").over(w_joined_on_month_by_risk_scor_2).alias("max_policy_premium_sum_1_prior"),
    )
)

line_level_policys = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.risk_score_adj",
        "s.max_policy_premium",
        "s.prev3_max_policy_premium",
        "s.region_joined_on_year_max_risk_score_adj",
        "s.max_policy_premium_sum_1_prior",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    line_level_policys
    .groupBy("policy_id")
    .pivot("policy_status", ["CANCELLED", "LAPSED", "ACTIVE"])
    .agg(F.sum("prev3_max_policy_premium"))
)
