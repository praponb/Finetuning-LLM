from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-08-27").cast("date")) & (F.col("status").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

enriched = (
    base
    .filter(F.upper(F.trim(F.col("status_clean"))).like("E%"))
    .select(
        "*",
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
        F.concat_ws("-", F.col("status"), F.col("status_clean")).alias("status_status_clean_label"),
    )
)

result = (
    enriched
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "status_clean",
        "start_date_label",
        "status_status_clean_label",
    )
    .orderBy(F.col("start_date_label").asc_nulls_last())
)
