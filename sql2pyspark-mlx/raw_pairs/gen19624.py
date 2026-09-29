from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 10)
    .select(
        "product_id",
        "quantity",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)
