from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") > 13) & F.col("dept_id").isin([3, 12, 21, 34]) & (F.col("salary") < 67000))
    .groupBy("level", "job_title")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)
