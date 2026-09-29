from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "product_id",
        "quantity",
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_adjusted"),
    )
)
