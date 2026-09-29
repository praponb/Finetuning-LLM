from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at_year").desc(), F.col("id").desc())

scoped = (
    df_meter_readings
    .filter(F.col("voltage") <= 239.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

deduped = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

labeled = (
    deduped
    .filter(F.col("kwh") < 90.6)
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

result = (
    labeled
    .groupBy("read_at")
    .pivot("quality", ["OK", "BAD"])
    .agg(F.max("voltage"))
    .orderBy("read_at")
)
