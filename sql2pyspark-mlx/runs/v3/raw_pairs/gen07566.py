from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(F.max("salary").alias("max_salary"))
    .filter(F.col("max_salary") < 146000)
    .select("hire_month", "max_salary")
)
