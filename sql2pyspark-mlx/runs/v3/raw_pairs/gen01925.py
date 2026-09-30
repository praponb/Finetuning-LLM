from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 156)
    .select("last_restock")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") <= "2025-08-01")
        .select(F.col("sale_date").alias("last_restock"))
    )
)
