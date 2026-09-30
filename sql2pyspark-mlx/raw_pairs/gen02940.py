from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.units").between(17, 24))
    .select(F.col("s.opened_date"), F.col("s.city"), F.col("ss.revenue"))
    .orderBy("revenue")
)
