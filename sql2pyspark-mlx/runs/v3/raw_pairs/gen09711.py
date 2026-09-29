from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.least(F.col("ss.units"), F.col("ss.revenue")).alias("least_units_revenue"),
    )
)
