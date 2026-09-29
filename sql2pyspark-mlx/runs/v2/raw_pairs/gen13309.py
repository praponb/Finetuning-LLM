from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Director", "Engineer", "Manager"])
    .agg(F.sum("salary"))
)
