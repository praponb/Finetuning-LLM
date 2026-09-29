from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(~F.col("s.city").isin(["Bangkok", "Singapore"]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.last_day(F.col("ss.sale_date")).alias("sale_date_month_end"),
    )
    .orderBy(F.col("revenue").desc())
)
