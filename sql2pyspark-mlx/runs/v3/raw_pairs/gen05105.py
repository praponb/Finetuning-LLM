from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 25)
    .withColumn("cost_per_stock_qty", (F.col("cost") / F.nullif("stock_qty", F.lit(0))))
    .withColumn("days_since_launch_date", F.datediff(F.current_date(), "launch_date"))
    .orderBy(F.col("id").desc())
)
