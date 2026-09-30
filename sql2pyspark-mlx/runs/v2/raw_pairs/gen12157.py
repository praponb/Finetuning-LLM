from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.upper(F.col("s.supplier_name")).like("%LOB%"))
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("city").desc(), F.col("rating").desc())
)
