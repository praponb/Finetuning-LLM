from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .filter(F.col("store_sales.product_id").isin([6, 7]))
    .select(
        F.col("stores.region"),
        F.col("stores.sqft"),
        F.col("stores.opened_date"),
        F.col("store_sales.sale_date"),
    )
    .orderBy("opened_date")
)
