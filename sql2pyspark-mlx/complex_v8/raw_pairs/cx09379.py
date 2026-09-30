from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").desc(), F.col("id").desc())
w_status = Window.partitionBy("status")

scoped_policys = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-11-20").cast("date"), F.lit("2027-03-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

unique_rows = (
    scoped_policys
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_windows = (
    unique_rows
    .select(
        "id",
        "product",
        "product_or_unknown",
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.round(F.avg("premium").over(w_status), 2).alias("status_avg_premium"),
    )
)

filtered = (
    with_windows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount").between(1273.7, 28426.9)), "left_semi")
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.min("status_avg_premium").alias("overall_min_status_avg_premium")))
    .filter(F.col("status_avg_premium") > F.col("overall_min_status_avg_premium"))
    .drop("overall_min_status_avg_premium")
)

product_or_unknown_stats = (
    without_matches
    .groupBy("product_or_unknown")
    .agg(F.avg("status_avg_premium").alias("avg_status_avg_premium"))
)

vs_group = (
    without_matches.alias("s")
    .join(product_or_unknown_stats.alias("g"), F.col("g.product_or_unknown") == F.col("s.product_or_unknown"), "inner")
    .filter(F.col("s.status_avg_premium") > F.col("g.avg_status_avg_premium"))
    .select(
        "s.*",
        "g.avg_status_avg_premium",
        F.round(F.col("s.status_avg_premium") - F.col("g.avg_status_avg_premium"), 2).alias("status_avg_premium_minus_avg"),
    )
)

result = (
    vs_group
    .groupBy("product_or_unknown")
    .pivot("product", ["TRAVEL", "HEALTH", "HOME"])
    .agg(F.sum("status_avg_premium"))
    .orderBy("product_or_unknown")
)
