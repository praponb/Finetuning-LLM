from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "quantity",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)
