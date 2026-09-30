from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.year(F.col("s.opened_date")) == 2024)
    .select(F.col("ss.sale_date"), F.col("s.city"), F.col("s.opened_date"))
    .orderBy(F.col("sale_date").desc())
)
