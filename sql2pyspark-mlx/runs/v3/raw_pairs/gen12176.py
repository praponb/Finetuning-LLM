from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.opened_date") > "2023-12-03")
    .select(
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
    )
    .orderBy("city", "sale_date")
)
