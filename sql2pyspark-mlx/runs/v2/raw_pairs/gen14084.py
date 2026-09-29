from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.col("hire_date") <= "2024-03-01") & ((F.col("salary") >= 125000) | F.col("email").isNotNull())))
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)
