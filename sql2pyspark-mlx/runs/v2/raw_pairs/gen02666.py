from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 109000)
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)
