from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating").isNull())
    .select(
        F.col("s.rating"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("p.supplier_id"),
    )
)
