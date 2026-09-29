from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.year("hire_date") >= 2023)
    .groupBy("level")
    .pivot("job_title", ["Engineer", "Analyst", "Manager"])
    .agg(F.max("salary"))
    .orderBy("level")
)
