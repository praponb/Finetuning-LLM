from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.filter(F.year("opened_date") >= 2024).alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left_semi")
    .select(F.col("store_sales.id"), F.col("store_sales.store_id"))
    .orderBy("id", "store_id")
)
