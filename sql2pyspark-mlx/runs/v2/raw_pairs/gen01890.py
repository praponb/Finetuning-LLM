from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Analyst", "Designer"]))
    .groupBy("level")
    .pivot("job_title", ["Director", "Designer", "Intern", "Manager"])
    .agg(F.count("salary"))
)
