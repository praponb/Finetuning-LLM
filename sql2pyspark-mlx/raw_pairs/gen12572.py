from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-04-23"))) & F.col("page").isNotNull())
    .select(
        "*",
        F.lower("page").alias("page_lower"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

result = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_7d"),
        F.max("duration_sec").over(w3).alias("duration_sec_max_30d"),
    )
)
