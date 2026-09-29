from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.month("last_restock").alias("last_restock_month"),
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
    .agg(F.min("last_restock").alias("first_last_restock"))
)
