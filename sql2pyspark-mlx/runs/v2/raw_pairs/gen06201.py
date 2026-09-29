from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["Singapore", "New York", "Berlin"]) & F.col("s.supplier_name").like("%City%"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.launch_date"),
        F.col("s.country"),
        F.col("s.rating"),
    )
    .orderBy("country", "id")
    .limit(10)
)
