from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .groupBy("level")
    .pivot("job_title", ["Director", "Intern", "Analyst"])
    .agg(F.count("salary"))
    .orderBy("level")
)
