from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.filter(F.col("units").between(21, 36)).alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left_anti")
    .filter(F.col("stores.store_name").like("%Flagship%"))
    .select(F.col("stores.id"), F.col("stores.sqft"), F.col("stores.region"))
    .orderBy(F.col("id").desc())
)
