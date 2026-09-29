from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") > 499000)
    .drop("total_salary")
)
