from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Home")
    .select("stock_qty", F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.sum("stock_qty").alias("total_stock"))
)
