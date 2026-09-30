from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Engineer")
    .select("id", "salary", "is_remote")
    .orderBy("id", F.col("salary").desc())
    .limit(5)
)
