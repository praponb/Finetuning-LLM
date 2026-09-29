from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("stock_qty") == 151) & (F.col("supplier_id") == 38))
    .withColumn("tags_text", F.array_join("tags", ", "))
)

agg = (
    filtered_products
    .groupBy("category")
    .agg(F.max("launch_date").alias("latest_launch_date"))
)

result = agg.select("category", "latest_launch_date")
