from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id") == 5).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(F.col("s.opened_date") > "2025-03-02")
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.region"))
)
