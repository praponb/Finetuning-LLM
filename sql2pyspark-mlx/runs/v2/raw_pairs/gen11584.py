from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com") & F.col("email").isNotNull())
    .groupBy(
        F.quarter("hire_date").alias("hire_quarter"),
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
    )
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") < 57)
)
