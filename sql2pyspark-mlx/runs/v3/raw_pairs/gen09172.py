from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.city").isin(["Chiang Mai", "Singapore"]))
    .select(
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.rating"),
        F.coalesce(F.col("s.supplier_name"), F.lit("N/A")).alias("supplier_name_filled"),
    )
    .orderBy("supplier_name_filled", F.col("stock_qty").desc())
)
