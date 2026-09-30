from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "store_id",
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
    )
)
