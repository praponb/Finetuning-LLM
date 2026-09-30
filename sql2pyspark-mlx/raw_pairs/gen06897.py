from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("*", F.upper("product_name").alias("product_name_upper"))
)

result = (
    filtered
    .alias("s")
    .join(df_suppliers.alias("dim"), F.col("s.supplier_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.city").alias("city"), F.col("s.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.cost")).alias("max_cost"),
    )
)
