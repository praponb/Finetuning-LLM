from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("category").isin(["Fashion", "Sports", "Electronics"]) | (F.col("supplier_id") == 7))
    .withColumn("days_since_launch_date", F.datediff(F.current_date(), "launch_date"))
)

summary = (
    recent_products
    .groupBy("category")
    .agg(F.count("*").alias("product_count"))
)

result = (
    summary
    .filter(F.col("product_count") >= 5)
    .select("category", "product_count")
    .orderBy("product_count")
)
