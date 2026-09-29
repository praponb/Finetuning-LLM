from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.quarter("last_restock").alias("last_restock_quarter"),
        "store_id",
    )
    .agg(F.max("quantity").alias("max_quantity"))
)
