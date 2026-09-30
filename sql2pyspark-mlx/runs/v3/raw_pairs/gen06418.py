from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 2.7) & ~F.col("supplier_name").like("%Par%"))
    .select("id", "rating", "country")
    .orderBy(F.col("rating").desc())
)
