from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "right")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.col("store_sales.sale_date").between("2023-07-01", "2023-09-06"))
    .groupBy(F.date_trunc("MONTH", F.col("stores.opened_date")).alias("opened_month"))
    .agg(F.min(F.col("store_sales.sale_date")).alias("min_sale_date"))
    .orderBy(F.col("min_sale_date").desc(), "opened_month")
    .limit(20)
)
