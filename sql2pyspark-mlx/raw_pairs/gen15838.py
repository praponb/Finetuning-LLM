from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNull() & F.col("duration_sec").between(228, 397))
    .select(
        "page",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
)
