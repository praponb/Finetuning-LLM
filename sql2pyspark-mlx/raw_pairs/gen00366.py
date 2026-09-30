from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.sale_date") > "2023-07-01")
    .select(
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("s.city"),
        F.col("s.region"),
        F.date_trunc("month", F.col("s.opened_date")).alias("opened_date_period"),
    )
)
