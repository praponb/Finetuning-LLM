from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("ss.sale_date") > "2023-03-01") & (F.col("i.last_restock") > "2024-10-13"))
    .select(
        F.col("i.store_id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
    )
    .orderBy(F.col("revenue").desc())
)
