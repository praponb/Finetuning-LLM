from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy(F.col("days_end_date_to_start_date").desc(), F.col("id").desc())

filtered_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-10-12").cast("date")) & (F.col("product") == "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

deduped = (
    filtered_policys
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

filtered = (
    deduped.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") >= F.lit("2024-08-18").cast("date")), "left_anti")
)

result = (
    filtered
    .rollup("status", "holder_id", "product")
    .agg(
        F.sum("days_end_date_to_start_date").alias("total_days_end_date_to_start_date"),
        F.round(F.avg("days_end_date_to_start_date"), 2).alias("avg_days_end_date_to_start_date"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "holder_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "total_days_end_date_to_start_date",
        "avg_days_end_date_to_start_date",
    )
)
