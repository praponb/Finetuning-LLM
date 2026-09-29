from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.product_id").isin([3, 4, 9]))
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.date_trunc("month", F.col("ss.sale_date")).alias("sale_date_period"),
    )
)
