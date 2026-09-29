from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("level", F.year("hire_date").alias("hire_year"), "is_remote")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.min("hire_date").alias("min_hire_date"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") >= 58)
)
