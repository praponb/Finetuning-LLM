from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"), "is_remote")
    .agg(
        F.min("salary").alias("min_salary"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("min_salary") > 137000)
    .orderBy("min_salary")
)
