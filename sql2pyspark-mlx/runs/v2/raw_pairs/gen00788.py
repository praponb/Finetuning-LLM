from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([21, 24]) & (F.col("salary") <= 87000))
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
    .withColumn("email_prefix", F.substring("email", 1, 3))
)
