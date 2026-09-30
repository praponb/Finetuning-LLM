from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("dept_id").isin([8, 33]))
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)
