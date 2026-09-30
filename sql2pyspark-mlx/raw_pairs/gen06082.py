from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(188, 246) & (F.col("stock_qty") < 119) & (F.col("stock_qty") >= 254))
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost")).otherwise(0)).alias("globex_cost"),
    )
)
