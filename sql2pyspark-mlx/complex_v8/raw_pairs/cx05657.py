from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy(F.col("premium_minus_avg").desc(), F.col("id").desc())

filtered = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-02-13").cast("date")) & (F.col("product") == "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("CANCELLED", "EXPIRED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

status_group_stats = (
    filtered
    .groupBy("status_group")
    .agg(F.avg("premium").alias("avg_premium"))
)

outliers = (
    filtered.alias("s")
    .join(status_group_stats.alias("g"), F.col("g.status_group") == F.col("s.status_group"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

screened = (
    outliers.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.year(F.col("p.joined_on")) == 2024), "left_semi")
)

latest_per_key = (
    screened
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        "status",
        "days_start_date_to_end_date",
        "start_date_quarter",
        "avg_premium",
        F.when(F.col("premium_minus_avg") >= 2000.0, "large").when(F.col("premium_minus_avg") >= -500.0, "medium").otherwise("small").alias("premium_minus_avg_tier"),
    )
    .orderBy(F.col("days_start_date_to_end_date").asc(), F.col("product").desc())
)
