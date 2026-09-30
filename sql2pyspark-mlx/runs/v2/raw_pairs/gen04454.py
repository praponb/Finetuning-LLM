from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id") == 1).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(F.col("s.city").isin(["Tokyo", "Chiang Mai", "Singapore"]))
    .select(F.col("s.id"), F.col("s.sqft"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
