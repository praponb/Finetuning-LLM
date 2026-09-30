from pyspark.sql import functions as F

result = (
    df_employees
    .filter((~F.col("is_remote") & F.col("is_remote")) | F.col("dept_id").isin([36, 37]))
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.min("salary").alias("min_salary"))
    .filter(F.col("min_salary") > 75000)
)
