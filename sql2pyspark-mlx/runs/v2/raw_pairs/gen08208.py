from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.7)
    .select("country")
    .distinct()
    .orderBy(F.col("country").desc())
)
