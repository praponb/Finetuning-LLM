from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 6, 9]))
    .select(
        "id",
        "store_id",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
    )
)
