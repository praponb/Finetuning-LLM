from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("s.rating") >= 2.6) | (F.col("ss.sale_date") > "2023-11-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.date_format(F.col("p.launch_date"), "MMM yyyy").alias("launch_date_label"),
    )
)
