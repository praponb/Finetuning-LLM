from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.city").isin(["Chiang Mai", "New York"]))
    .select(
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("p.price"),
    )
)
