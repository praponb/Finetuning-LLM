from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.brand") == "Acme")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("s.city"),
        F.col("s.rating"),
        F.when(F.col("s.city").isin(["Chiang Mai", "New York"]), "Priority").otherwise("Other").alias("city_group"),
    )
    .orderBy(F.col("launch_date").desc(), F.col("city_group").desc())
)
