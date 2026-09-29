from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.filter(F.col("units") < 28).alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left_semi")
    .filter(F.col("stores.opened_date") >= "2025-10-01")
    .select(F.col("stores.id"), F.col("stores.store_name"), F.col("stores.sqft"))
    .orderBy(F.col("store_name").desc(), "id")
)
