from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .groupBy(
        F.date_trunc("MONTH", F.col("stores.opened_date")).alias("opened_month"),
        F.col("stores.region"),
    )
    .agg(F.count(F.col("store_sales.units")).alias("units_count"))
)
