from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.country").isin(["JP", "UK"]) | F.col("p.product_name").like("%Sma%"))
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.when(F.col("s.city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
    )
    .orderBy(F.col("stock_qty").desc())
)
