from pyspark.sql import functions as F

src = (
    df_products
    .filter(F.array_contains("tags", "clearance") & (F.col("brand") == "Globex"))
    .withColumn("price_int", F.col("price").cast("int"))
)

summary = (
    src
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
)

result = summary.select("launch_month", "total_stock_qty")
