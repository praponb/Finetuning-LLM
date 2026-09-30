from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date").between("2024-04-01", "2025-02-01"))
    .select(
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("s.opened_date"),
        F.date_sub(F.col("ss.sale_date"), 27).alias("sale_date_minus_27d"),
    )
    .orderBy("sale_date_minus_27d", "opened_date")
)
