from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
