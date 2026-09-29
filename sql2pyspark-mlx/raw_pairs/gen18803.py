from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.length(F.col("s.store_name")) < 13)
    .select(
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.last_day(F.col("ss.sale_date")).alias("sale_date_month_end"),
    )
    .orderBy("sqft")
)
