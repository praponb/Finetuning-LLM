from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 2.0) | (F.col("country") == "US"))
    .select("id", "country")
    .orderBy(F.col("country").desc())
)
