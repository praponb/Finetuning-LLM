from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("store_name").isNotNull()).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.sale_date") > "2024-05-01")
    .select(F.col("ss.id"), F.col("ss.store_id"), F.col("ss.units"))
)
