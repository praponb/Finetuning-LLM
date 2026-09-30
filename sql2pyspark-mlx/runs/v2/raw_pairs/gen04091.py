from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("s.rating"),
        F.col("s.country"),
    )
    .orderBy("country")
)
