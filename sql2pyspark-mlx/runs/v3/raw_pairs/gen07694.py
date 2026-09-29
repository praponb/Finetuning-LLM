from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("hire_date") < "2023-11-01")
    .withColumn("adjusted_salary", F.round((F.col("salary") * 1.07), 2))
)

summary = (
    valid_employees
    .groupBy("level")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    summary
    .filter(F.col("total_salary") > 415000)
    .select("level", "total_salary")
)
