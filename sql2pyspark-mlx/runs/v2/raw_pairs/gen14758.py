from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
)
