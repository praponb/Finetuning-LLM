from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(160, 476))
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
    .orderBy(F.col("unique_pages").desc())
)
