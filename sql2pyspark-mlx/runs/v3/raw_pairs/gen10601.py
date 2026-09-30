from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isNotNull())
    .select("city", F.lower("country").alias("country_lower"))
    .orderBy(F.col("country_lower").desc(), F.col("city").desc())
)
