from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("city").isin(["Tokyo", "Bangkok"])).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.revenue") <= 1100)
    .select(F.col("ss.id"), F.col("ss.store_id"), F.col("ss.revenue"))
    .orderBy("revenue")
)
