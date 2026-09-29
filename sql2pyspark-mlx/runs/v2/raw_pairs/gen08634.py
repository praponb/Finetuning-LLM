from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/cart", "/checkout"]) & F.col("page").isin(["/cart", "/product"]))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .orderBy(F.col("max_duration_sec").desc())
)
