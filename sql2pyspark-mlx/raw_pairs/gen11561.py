from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("s.sqft") <= 8494) | (F.col("p.brand") != "Globex"))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("s.store_name"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        (F.col("p.price") - F.col("p.cost")).alias("margin"),
    )
)
