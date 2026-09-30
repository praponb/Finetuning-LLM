from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull() | F.col("dept_id").isin([6, 30, 37]))
    .groupBy(F.month("hire_date").alias("hire_month"), "manager_id")
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
    )
    .orderBy("l4_salary")
)
