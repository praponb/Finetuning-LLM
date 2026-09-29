from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("s.rating"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
    )
)
