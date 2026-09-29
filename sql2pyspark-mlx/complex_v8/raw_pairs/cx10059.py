from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_status_holder_id_by_holder_r = Window.partitionBy("status", "holder_id").orderBy(F.col("holder_risk_score").desc(), "id")
w_holder_id_by_holder_risk_sco = Window.partitionBy("holder_id").orderBy("holder_risk_score_share_of_status", "id")
w_holder_id_by_holder_risk_sco_2 = Window.partitionBy("holder_id").orderBy("holder_risk_score_share_of_status", "id").rowsBetween(-3, Window.currentRow)
w_holder_id_holder_risk_score = Window.partitionBy("holder_id", "holder_risk_score_tercile")

cleaned = (
    df_policies
    .filter((F.year(F.col("end_date")) == 2026) & (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.approved_amount").between(0.0, 15857.9)), "left_semi")
)

enriched_holder = (
    with_activity.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.days_since_end_date",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

ranked = (
    enriched_holder
    .select(
        "*",
        F.round(F.col("days_since_end_date") / F.nullif(F.sum("days_since_end_date").over(w_status), F.lit(0)), 4).alias("days_since_end_date_share_of_status"),
        F.ntile(3).over(w_status_holder_id_by_holder_r).alias("holder_risk_score_tercile"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_status), F.lit(0)), 4).alias("holder_risk_score_share_of_status"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
    )
)

with_activity_2 = (
    ranked.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
)

labeled = (
    with_activity_2
    .select(
        "*",
        F.round(F.col("holder_risk_score") / F.nullif(F.col("days_since_end_date"), F.lit(0)), 3).alias("holder_risk_score_per_days_since_end_date"),
        F.when(F.col("days_since_end_date") >= 490, "large").when(F.col("days_since_end_date") >= 280, "medium").otherwise("small").alias("days_since_end_date_tier"),
    )
)

sequenced = (
    labeled
    .select(
        "id",
        "holder_id",
        "status",
        "days_since_end_date",
        "holder_region",
        "days_since_end_date_share_of_status",
        "holder_risk_score_share_of_status",
        "days_since_end_date_tier",
        F.rank().over(w_holder_id_by_holder_risk_sco).alias("holder_risk_score_share_of_status_rank"),
        F.max("rows_in_status").over(w_holder_id_by_holder_risk_sco_2).alias("rows_in_status_max_4"),
        F.max("days_since_end_date").over(w_holder_id_holder_risk_score).alias("holder_id_holder_risk_score_tercile_max_days_sin"),
        F.size(F.collect_set("holder_region").over(w_holder_id_holder_risk_score)).alias("distinct_holder_region_in_holder_id_holder_risk_"),
    )
)

result = (
    sequenced
    .filter(F.col("holder_region") != "East")
    .select(
        "id",
        "holder_id",
        "days_since_end_date",
        "holder_region",
        "days_since_end_date_share_of_status",
        "holder_risk_score_share_of_status",
        "days_since_end_date_tier",
        "rows_in_status_max_4",
        "holder_id_holder_risk_score_tercile_max_days_sin",
        "distinct_holder_region_in_holder_id_holder_risk_",
        F.when(F.col("status").isin("EXPIRED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.concat(F.col("status"), F.lit("#"), F.col("days_since_end_date").cast("string")).alias("status_ref"),
    )
    .orderBy(
        F.col("distinct_holder_region_in_holder_id_holder_risk_").desc(),
        F.col("holder_region").desc(),
    )
)
