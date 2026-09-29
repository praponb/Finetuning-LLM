from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        "store_id",
        F.when(F.col("quantity") >= 257, "high").when(F.col("quantity") >= 48, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
)
