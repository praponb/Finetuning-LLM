from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.7) & F.col("supplier_name").like("Lio%"))
    .select("id", "supplier_name", "city", F.round("rating", 1).alias("rating_rounded"))
    .orderBy("city")
)
