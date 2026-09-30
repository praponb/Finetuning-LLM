from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 4) & (F.col("hire_date") < "2024-02-01"))
    .groupBy(F.year("hire_date").alias("hire_year"), "level")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
    )
)
