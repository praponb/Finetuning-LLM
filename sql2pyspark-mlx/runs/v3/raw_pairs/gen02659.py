from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "is_remote", "email", F.ceil("salary").alias("salary_whole"))
    .orderBy("email")
)
