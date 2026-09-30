from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 7)
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") >= 31)
    .select("product_id", "last_restock_month", "quantity_range")
    .orderBy("last_restock_month")
)
