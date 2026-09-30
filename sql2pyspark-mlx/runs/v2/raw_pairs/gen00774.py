from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([1, 23, 31, 35]))
    .groupBy("manager_id")
    .pivot("job_title", ["Designer", "Intern", "Director"])
    .agg(F.max("salary"))
)
