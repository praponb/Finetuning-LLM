from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .select(
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.date_add(F.col("ss.sale_date"), 83).alias("sale_date_plus_83d"),
    )
)
