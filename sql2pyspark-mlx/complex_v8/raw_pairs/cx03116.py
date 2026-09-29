from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc(), F.col("id").desc())

scoped = (
    df_meter_readings
    .filter(F.col("kwh") >= 35.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

deduped = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupBy("site_id")
    .pivot("quality", ["OK", "EST", "BAD"])
    .agg(F.count("days_since_read_at"))
)
