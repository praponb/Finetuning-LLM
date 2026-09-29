from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Initech", "Umbrella"]) | (F.size("tags") > 1))
    .withColumn("cost_times_stock_qty", (F.col("cost") * F.col("stock_qty")))
)
