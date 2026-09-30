from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .groupBy(F.col("stores.id"), F.col("stores.store_name"))
    .agg(
        F.count(F.col("store_sales.id")).alias("store_sale_count"),
        F.min(F.col("store_sales.sale_date")).alias("first_sale_date"),
    )
    .orderBy("store_name")
)
