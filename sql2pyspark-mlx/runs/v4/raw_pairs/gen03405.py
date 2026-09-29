from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.month("hire_date").isin([4, 9, 12]) & ~F.col("is_remote")))
    .select("id", "job_title")
    .orderBy("job_title")
)
