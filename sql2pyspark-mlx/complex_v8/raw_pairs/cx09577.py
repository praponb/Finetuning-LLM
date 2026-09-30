from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy("premium", "id")

cleaned_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-12-05").cast("date")) | (F.col("holder_id") <= 6))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.when(F.col("product").isin("TRAVEL", "LIFE"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
    )
)

with_activity = (
    cleaned_policys
    .crossJoin(cleaned_policys.agg(F.avg("premium").alias("overall_avg_premium")))
    .filter(F.col("premium") > F.col("overall_avg_premium"))
    .drop("overall_avg_premium")
)

unique_rows = (
    with_activity
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent_holder = (
    unique_rows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.product_group",
        "s.premium_distance",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    with_parent_holder
    .rollup("product", "product_group")
    .agg(
        F.sum(F.when(~F.col("status").isin("EXPIRED", "ACTIVE"), F.col("holder_risk_score"))).alias("holder_risk_score_status_not_in_expired_active"),
        F.round(F.sum("holder_risk_score"), 2).alias("total_holder_risk_score"),
        F.round(100 * F.count(F.when(F.col("status") == "EXPIRED", 1)) / F.count("*"), 1).alias("pct_status_expired"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("product_group").asc_nulls_last(),
    )
)
