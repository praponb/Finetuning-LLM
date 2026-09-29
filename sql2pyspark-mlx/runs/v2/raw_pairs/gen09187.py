from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter((F.year(F.col("s.opened_date")) == 2024) | F.col("s.city").isin(["Tokyo", "Chiang Mai", "London"]))
    .select(F.col("s.store_name"), F.col("s.region"), F.col("ss.store_id"))
    .orderBy("store_id")
)
