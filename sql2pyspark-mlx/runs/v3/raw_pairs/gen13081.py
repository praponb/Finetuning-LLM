from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.upper(F.trim(F.col("s.supplier_name"))) == "LION CITY IMPORTS")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.cost"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("s.country"),
        F.date_format(F.col("p.launch_date"), "MMM yyyy").alias("launch_date_label"),
    )
    .orderBy("city")
)
