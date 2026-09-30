from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.quarter("last_restock").alias("last_restock_quarter"),
        F.when(F.col("quantity") >= 184, "high").when(F.col("quantity") >= 64, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("num_inventory"),
    )
)
