from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("cost") >= 360) & (F.col("brand") == "Stark") & (F.col("brand") == "Globex"))
)

agg = (
    filtered_products
    .groupBy("category")
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
)

result = (
    agg
    .select("category", "avg_stock_qty")
    .orderBy("category")
)
