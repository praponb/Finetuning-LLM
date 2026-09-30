from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.product_name").like("R%"))
    .select(
        F.col("p.cost"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy(F.col("product_name").desc())
)
