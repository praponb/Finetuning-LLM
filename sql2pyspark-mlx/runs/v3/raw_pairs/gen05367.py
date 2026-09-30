from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "country", "rating", "city")
    .orderBy(F.col("country").desc())
)
