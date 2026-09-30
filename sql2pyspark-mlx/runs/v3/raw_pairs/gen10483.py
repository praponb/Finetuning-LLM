from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") > 7)
    .groupBy("manager_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") < 484000)
)
