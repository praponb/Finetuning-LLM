from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(~(F.col("ss.product_id").isin([2, 7, 9]) | F.array_contains(F.col("p.tags"), "clearance")))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("p.price"),
        F.col("s.city"),
        F.col("s.sqft"),
    )
    .orderBy("sale_date")
)
