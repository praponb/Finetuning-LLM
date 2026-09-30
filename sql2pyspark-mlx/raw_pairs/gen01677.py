from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.quarter("last_restock").alias("last_restock_quarter"),
        F.when(F.col("quantity") >= 260, "high").when(F.col("quantity") >= 78, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc())
)
