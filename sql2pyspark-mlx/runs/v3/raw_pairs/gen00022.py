from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("s.opened_date"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("units").desc(), "launch_date")
)
