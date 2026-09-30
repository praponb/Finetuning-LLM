from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"),
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_adjusted"),
    )
    .orderBy(F.col("adjusted_quantity").desc(), "id")
    .limit(50)
)
