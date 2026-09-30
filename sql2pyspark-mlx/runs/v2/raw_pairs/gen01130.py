from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter((F.length(F.col("s.store_name")) >= 10) & (F.col("p.cost") < 340))
    .select(
        F.col("ss.sale_date"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("s.region"),
        F.col("s.city"),
    )
)
