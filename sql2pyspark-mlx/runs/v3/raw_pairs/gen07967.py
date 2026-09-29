from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.max("hire_date").alias("last_hire_date"),
        F.countDistinct("salary").alias("unique_managers"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") > 97000)
    .drop("avg_salary")
    .orderBy(F.col("unique_levels").desc())
)
