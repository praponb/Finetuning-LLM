from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 96)
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 269, "high").when(F.col("quantity") >= 112, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("quantity_range") < 53)
)
