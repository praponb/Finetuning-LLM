from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("product") != "HEALTH")
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
    )
)

rollup_base = (
    base
    .groupBy(
        F.year("end_date_month").alias("end_date_month_year"),
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
        "product",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
)

annotated = (
    rollup_base
    .select(
        "end_date_month_year",
        "end_date_ym",
        "product",
        "distinct_holder_ids",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

result = (
    annotated
    .groupBy("distinct_holder_ids")
    .pivot("product", ["HOME", "AUTO", "TRAVEL"])
    .agg(F.sum("end_date_month_year"))
    .orderBy("distinct_holder_ids")
)
