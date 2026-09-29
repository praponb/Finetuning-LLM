from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

rollup_base = (
    scoped
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        F.year("start_date").alias("start_date_year"),
        "holder_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("start_date").alias("first_start_date"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.max("premium").alias("max_premium"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
    )
    .filter(F.col("cnt") >= 3)
)

result = (
    rollup_base
    .select(
        "end_date_month",
        "start_date_year",
        "holder_id",
        "cnt",
        "distinct_ids",
        "first_start_date",
        "end_date_span_days",
        "max_premium",
        F.round(F.col("max_premium") * 1.1, 2).alias("max_premium_adj"),
        F.when(F.col("max_premium") >= 3080.0, "A").when(F.col("max_premium") >= 1880.0, "B").otherwise("C").alias("max_premium_tier"),
    )
    .orderBy(F.col("max_premium_tier").desc())
)
