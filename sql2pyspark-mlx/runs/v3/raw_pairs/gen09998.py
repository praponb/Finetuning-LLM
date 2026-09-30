from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "clearance") & F.col("p.stock_qty").between(130, 178))
    .select(
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("s.city"),
        F.col("s.sqft"),
    )
)
