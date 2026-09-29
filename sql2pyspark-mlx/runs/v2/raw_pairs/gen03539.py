from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com") & (F.col("salary") <= 91000))
    .groupBy("manager_id", "level")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.min("salary").alias("min_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("unique_depts") < 5)
)
