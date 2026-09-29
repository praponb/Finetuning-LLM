from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_start_date_year_by_ri = Window.partitionBy(F.year("policy_start_date")).orderBy(F.col("risk_score").desc(), "id", "policy_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_start_date_year_by_ri_2 = Window.partitionBy(F.year("policy_start_date")).orderBy(F.col("risk_score").desc(), "id", "policy_id").rowsBetween(-1, Window.currentRow)
w_policy_id_policy_end_date_mo = Window.partitionBy("policy_id", F.month("policy_end_date"))

base = df_policyholders.filter(F.col("joined_on") < F.lit("2025-04-20").cast("date"))

enriched = base.withColumn("risk_score_adj", F.round(F.col("risk_score") * 0.9, 2))

exploded_detail = (
    enriched.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_name",
        "s.region",
        "s.risk_score",
        "s.risk_score_adj",
        F.col("p.id").alias("policy_id"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

sequenced = (
    exploded_detail
    .select(
        "id",
        "holder_name",
        "risk_score",
        "risk_score_adj",
        "policy_id",
        "policy_start_date",
        "policy_end_date",
        "policy_premium",
        "policy_status",
        F.nth_value("policy_end_date", 2).over(w_policy_start_date_year_by_ri).alias("second_policy_end_date"),
        F.round(F.avg("policy_premium").over(w_policy_start_date_year_by_ri_2), 2).alias("policy_premium_avg_2"),
        F.count(F.lit(1)).over(w_policy_id_policy_end_date_mo).alias("rows_in_policy_id_policy_end_date_month"),
        F.last("risk_score").over(w_policy_start_date_year_by_ri).alias("last_risk_score"),
        F.max("risk_score_adj").over(w_policy_id_policy_end_date_mo).alias("policy_id_policy_end_date_month_max_risk_score_a"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") != "PAID"), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "holder_name",
        "risk_score",
        "risk_score_adj",
        "policy_id",
        "policy_end_date",
        "policy_premium",
        "policy_status",
        "second_policy_end_date",
        "policy_premium_avg_2",
        "rows_in_policy_id_policy_end_date_month",
        "policy_id_policy_end_date_month_max_risk_score_a",
        (F.col("policy_premium_avg_2") + F.col("risk_score")).alias("policy_premium_avg_2_plus_risk_score"),
    )
    .orderBy(F.col("policy_status").desc(), "policy_end_date")
)
