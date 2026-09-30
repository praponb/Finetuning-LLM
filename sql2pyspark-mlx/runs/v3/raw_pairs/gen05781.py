from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-03-01", "2024-02-14"))
    .select(
        "quantity",
        "store_id",
        "product_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)
