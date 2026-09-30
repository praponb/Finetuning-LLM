from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Car%"))
    .withColumn("salary_rounded", F.round("salary", 2))
    .orderBy("level", "id")
    .limit(5)
)
