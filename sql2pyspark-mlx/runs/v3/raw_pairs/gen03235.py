from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("store_name")
    .intersect(df_stores.filter(~F.col("store_name").like("M%")).select("store_name"))
    .orderBy(F.col("store_name").desc())
)
