from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("revenue") > 2900).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.sqft"), F.col("s.opened_date"))
    .orderBy(F.col("id").desc())
)
