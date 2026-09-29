from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("email").isNotNull() & ~F.col("is_active")) | (F.col("country") == "TH"))
    .select("id", "age", "country", "city", F.length("country").alias("country_length"))
    .orderBy(F.col("country").desc())
)
