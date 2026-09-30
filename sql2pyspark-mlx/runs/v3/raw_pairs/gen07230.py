from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .select(
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
)
