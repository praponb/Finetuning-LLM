from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.country").isin(["UK", "SG", "DE"]) & F.month(F.col("p.launch_date")).isin([1, 3, 4]))
    .select(
        F.col("s.city"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("p.product_name"),
    )
)
