from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNull())
    .select("id", "credit_limit", "country", F.upper("name").alias("name_upper"))
    .orderBy(F.col("credit_limit").desc())
)
