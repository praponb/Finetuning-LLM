from pyspark.sql import functions as F

src = (
    df_inventory
    .filter((F.col("product_id") == 2) & (F.col("product_id") == 9) & (F.col("last_restock") > "2024-02-01"))
)

summary = (
    src
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        "product_id",
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    summary
    .select("last_restock_month", "product_id", "quantity_range")
    .orderBy("quantity_range", F.col("last_restock_month").desc())
)
