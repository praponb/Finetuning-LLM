from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("rating").between(2.8, 3.5)).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.cost") <= 710)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.brand"),
    )
)
