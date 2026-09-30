from pyspark.sql import functions as F

src = df_web_events.filter(F.col("session_id").isin([28, 30, 35]))

summary = (
    src
    .groupBy("event_type", "page")
    .agg(F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"))
)

result = (
    summary
    .filter(F.col("product_count") > 4)
    .select("event_type", "page", "product_count")
    .orderBy(F.col("page").desc())
)
