from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 1.07), 2).alias("adjusted_quantity"),
    )
)
