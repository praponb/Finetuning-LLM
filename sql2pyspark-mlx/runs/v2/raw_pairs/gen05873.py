from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 133)
    .select(
        "id",
        "quantity",
        "last_restock",
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
    )
    .orderBy(F.col("id").desc())
)
