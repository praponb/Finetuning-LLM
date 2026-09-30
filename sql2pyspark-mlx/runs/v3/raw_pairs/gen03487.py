from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "outer")
    .filter(F.col("s.opened_date") <= "2023-11-01")
    .select(
        F.col("ss.units"),
        F.col("s.store_name"),
        F.col("p.launch_date"),
        F.col("p.price"),
    )
)
