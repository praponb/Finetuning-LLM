from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .pivot("job_title", ["Designer", "Engineer", "Manager"])
    .agg(F.avg("salary"))
    .orderBy("is_remote")
)
