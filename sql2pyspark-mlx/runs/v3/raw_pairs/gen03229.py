from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(75, 167) & F.col("product_id").isin([2, 3, 4, 10]) & F.col("store_id").isin([4, 5, 6]))
    .select(
        "id",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
    .orderBy("id")
)
