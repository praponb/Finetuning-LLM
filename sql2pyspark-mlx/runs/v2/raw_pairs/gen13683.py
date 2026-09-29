from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"), "is_remote")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") > 590000)
    .orderBy(F.col("hire_month").desc(), "is_remote")
    .limit(10)
)
