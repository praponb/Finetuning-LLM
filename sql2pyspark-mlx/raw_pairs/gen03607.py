from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "US", "UK"]))
    .select("id", "country", "city")
    .orderBy(F.col("id").desc())
)
