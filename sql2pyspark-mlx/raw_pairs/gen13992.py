from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(~F.col("p.brand").isin(["Umbrella", "Acme"]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("p.brand"),
        F.col("s.rating"),
        F.col("s.country"),
    )
    .orderBy("product_id", "id")
    .limit(10)
)
