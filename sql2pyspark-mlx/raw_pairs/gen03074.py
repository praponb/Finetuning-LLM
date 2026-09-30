from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .pivot("job_title", ["Engineer", "Manager", "Intern"])
    .agg(F.max("salary"))
    .orderBy("hire_quarter")
)
