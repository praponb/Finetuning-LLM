from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-07-07").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj_2"),
        F.month(F.col("start_date")).alias("start_date_month"),
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

with_activity = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
    .filter(F.col("premium_adj_2") > 2390.0)
)

result = (
    with_activity
    .rollup("start_date_month", "status", "product")
    .agg(F.round(F.stddev("premium_adj"), 2).alias("stddev_premium_adj"))
    .select(
        "start_date_month",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "stddev_premium_adj",
    )
    .orderBy(
        F.col("start_date_month").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
