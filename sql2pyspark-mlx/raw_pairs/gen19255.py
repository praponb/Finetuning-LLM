from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter((F.col("ss.sale_date") < "2023-05-01") & (F.col("s.country") != "US"))
    .select(
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.upper(F.trim(F.col("p.product_name"))).alias("product_name_normalized"),
    )
)
