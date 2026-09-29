from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([3, 4, 11]))
    .withColumn("stock_qty_times_cost", (F.col("stock_qty") * F.col("cost")))
    .withColumn("margin", F.round((F.col("cost") / F.col("price")), 2))
)
