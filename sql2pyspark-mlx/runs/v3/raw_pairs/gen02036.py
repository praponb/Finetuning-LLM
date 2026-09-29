from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.city").isin(["Tokyo", "Berlin"]))
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(F.count(F.col("store_sales.units")).alias("units_count"))
)
