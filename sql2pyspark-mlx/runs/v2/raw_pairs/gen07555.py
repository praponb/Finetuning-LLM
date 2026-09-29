from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("sale_date") >= "2024-03-18").alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(~F.col("s.region").isin(["West", "South"]))
    .select(F.col("s.id"), F.col("s.city"))
)
