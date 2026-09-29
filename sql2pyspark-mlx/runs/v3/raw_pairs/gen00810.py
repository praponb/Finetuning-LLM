from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Intern", "Designer"]))
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
    .orderBy(F.col("engineer_count").desc())
)
