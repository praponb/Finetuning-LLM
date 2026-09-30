from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("s.rating") >= 4.0) & F.col("p.product_name").isNotNull())
    .select(
        F.col("s.rating"),
        F.col("s.city"),
        F.col("p.launch_date"),
        F.date_format(F.col("p.launch_date"), "MMM yyyy").alias("launch_date_label"),
    )
    .orderBy(F.col("rating").desc())
)
