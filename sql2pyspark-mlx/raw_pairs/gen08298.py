from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Umbrella", "Stark"]))
    .withColumn("stock_qty_plus_cost", (F.col("stock_qty") + F.col("cost")))
)
