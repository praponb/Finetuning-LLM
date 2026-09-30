from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 184) & (F.col("last_restock") <= "2025-03-01"))
    .groupBy(
        F.when(F.col("quantity") >= 182, "high").when(F.col("quantity") >= 78, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
