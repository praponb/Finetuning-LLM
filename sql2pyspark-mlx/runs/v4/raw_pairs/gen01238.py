from pyspark.sql import functions as F

result = (
    df_employees
    .select("salary", F.trim("level").alias("clean_level"))
    .orderBy("salary")
)
