from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.units") == 25)
    .select(
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("p.launch_date"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
)
