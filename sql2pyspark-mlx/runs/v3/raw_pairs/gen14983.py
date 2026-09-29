from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Engineer", "Director"]))
    .groupBy("level")
    .agg(F.max("salary").alias("max_salary"))
    .filter(F.col("max_salary") > 106000)
    .select("level", "max_salary")
    .orderBy("max_salary")
)
