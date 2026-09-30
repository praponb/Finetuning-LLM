from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") >= 41)
    .select("segment", "country")
    .distinct()
    .orderBy(F.col("country").desc())
)
