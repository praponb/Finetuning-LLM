from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 700)
    .select(F.col("product_name").alias("label"))
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([23, 33, 38]))
        .select(F.col("event_type").alias("label"))
    )
    .orderBy("label")
)
