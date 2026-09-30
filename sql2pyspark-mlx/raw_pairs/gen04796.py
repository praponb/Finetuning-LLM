from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%ARO%") & (F.col("dept_id") == 22))
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
    .filter(F.col("avg_salary") < 123000)
    .orderBy("dept_id")
)
