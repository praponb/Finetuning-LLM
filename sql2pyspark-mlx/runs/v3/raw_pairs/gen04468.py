from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Engineer")
    .groupBy("level")
    .pivot("job_title", ["Engineer", "Analyst", "Designer"])
    .agg(F.avg("salary"))
    .orderBy("level")
)
