from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("product").isin("TRAVEL", "HOME")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

screened = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.lower(F.col("p.holder_name")).like("%wo%")) & (F.col("p.joined_on") >= F.lit("2024-07-18").cast("date")), "left_semi")
)

rollup_base = (
    screened
    .filter(F.col("product_slug").isNotNull())
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "product",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("status").alias("distinct_status"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    rollup_base
    .groupBy("n_rows")
    .pivot("product", ["HEALTH", "LIFE", "AUTO", "HOME"])
    .agg(F.max("distinct_status"))
    .orderBy("n_rows")
)
