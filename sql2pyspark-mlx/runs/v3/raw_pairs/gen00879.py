from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 5, 10]))
    .groupBy(
        "store_id",
        F.month("last_restock").alias("last_restock_month"),
        F.when(F.col("quantity") >= 265, "high").when(F.col("quantity") >= 46, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy(F.col("total_quantity").desc())
)
