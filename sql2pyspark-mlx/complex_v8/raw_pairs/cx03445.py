from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

summary = (
    recent_policys
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "status",
    )
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("row_count"),
        F.min("start_date").alias("first_start_date"),
    )
)

scored = (
    summary
    .filter(F.col("status") != "CANCELLED")
    .select(
        "start_date_month",
        "end_date_month",
        "status",
        "row_count",
        F.date_format(F.col("first_start_date"), "MMM yyyy").alias("first_start_date_label"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_start_date")).alias("days_since_first_start_date"),
    )
)

result = (
    scored
    .select(
        "start_date_month",
        "row_count",
        "status_prefix",
        F.when(F.col("row_count") >= 9, "A").when(F.col("row_count") >= 5, "B").otherwise("C").alias("row_count_tier"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
    .orderBy("status_prefix", F.col("start_date_month").desc())
)
