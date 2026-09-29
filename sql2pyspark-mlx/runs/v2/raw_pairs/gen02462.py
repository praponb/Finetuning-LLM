from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").isNotNull() & F.col("s.city").isin(["New York", "Chiang Mai"]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("s.opened_date"),
    )
)
