from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.supplier_name").like("Si%") & F.col("p.product_name").like("Co%"))
    .select(
        F.col("s.rating"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.ceil(F.col("s.rating")).alias("rating_whole"),
    )
    .orderBy("product_name")
)
