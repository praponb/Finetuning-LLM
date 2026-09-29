from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_region", "holder_segment").orderBy(F.col("days_since_start_date").desc(), F.col("id").desc())

cleaned_policys = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-10-12").cast("date"), F.lit("2026-09-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

labeled = (
    cleaned_policys
    .select(
        "*",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

with_details_holder = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_prefix",
        "s.days_since_start_date",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

deduped = (
    with_details_holder
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .filter(F.col("product_prefix").isNotNull())
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "product_prefix",
        "days_since_start_date",
        "holder_name",
        "holder_region",
        F.date_trunc("year", F.col("end_date")).alias("end_date_year"),
        F.date_trunc("week", F.col("start_date")).alias("start_date_week"),
    )
    .orderBy("end_date", "id")
    .limit(10)
)
