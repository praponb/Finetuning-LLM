from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .filter((F.col("s.sqft") != 9305) & F.col("p.cost").between(300, 410))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.price"),
        F.col("s.opened_date"),
    )
)
