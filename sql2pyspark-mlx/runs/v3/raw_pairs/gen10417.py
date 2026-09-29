from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select("city")
    .distinct()
    .orderBy(F.col("city").desc())
)
