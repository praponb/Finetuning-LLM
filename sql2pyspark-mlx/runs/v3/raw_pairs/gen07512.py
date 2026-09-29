from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 10]))
    .select(
        "store_id",
        "quantity",
        "product_id",
        F.round((F.col("quantity") * 100), 2).alias("adjusted_quantity"),
    )
)
