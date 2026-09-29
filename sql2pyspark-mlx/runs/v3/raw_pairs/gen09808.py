from pyspark.sql import functions as F

result = (
    df_products
    .filter(~((F.col("stock_qty") != 297) & (F.year("launch_date") == 2024) & F.array_contains("tags", "eco")))
    .select("id", "price", "stock_qty", F.col("cost").cast("int").alias("cost_int"))
    .orderBy("price")
)
