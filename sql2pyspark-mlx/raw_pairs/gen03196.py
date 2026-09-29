from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Director", "Intern", "Designer", "Analyst"])
    .agg(F.count("salary"))
    .orderBy("level")
)
