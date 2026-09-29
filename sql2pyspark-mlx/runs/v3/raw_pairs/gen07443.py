from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 6, 7, 9]) & F.col("product_id").isin([5, 6]))
    .select(
        "quantity",
        "store_id",
        "product_id",
        F.round((F.col("quantity") * 1.07), 2).alias("adjusted_quantity"),
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
    )
    .orderBy(F.col("adjusted_quantity").desc())
)
