from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .filter(~F.col("stores.city").isin(["Singapore", "Tokyo", "Bangkok"]))
    .select(F.col("stores.city"), F.col("store_sales.product_id"))
    .orderBy("product_id", "city")
)
