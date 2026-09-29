from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "full_name", F.length("email").alias("email_length"))
    .orderBy(F.col("id").desc())
    .limit(3)
)
