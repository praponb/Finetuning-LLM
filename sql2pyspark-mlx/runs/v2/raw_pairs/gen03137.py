from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper(F.trim("email")) == "IVY@GMAIL.COM")
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.max("salary").alias("max_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
    )
)
