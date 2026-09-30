from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("product_id") == 3)

last_restock_month_summary = (
    valid_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    last_restock_month_summary
    .filter(F.col("max_quantity") >= 110)
    .select("last_restock_month", "min_quantity", "max_quantity")
    .orderBy("last_restock_month")
)
