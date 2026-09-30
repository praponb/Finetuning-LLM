from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
)
