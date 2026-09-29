from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 2.4) & (F.col("rating") < 3.2))
    .select("city", "country", "rating")
    .orderBy(F.col("country").desc(), "rating")
)
