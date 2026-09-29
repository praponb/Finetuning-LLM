from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.month("last_restock").isin([3, 4, 12]))

last_restock_month_summary = (
    filtered_inventory
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        "store_id",
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    last_restock_month_summary
    .filter(F.col("total_quantity") > 480)
    .select("last_restock_month", "store_id", "min_quantity", "total_quantity")
    .orderBy(F.col("total_quantity").desc())
)
