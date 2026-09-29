from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("manager_id", "level")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.min("salary").alias("min_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .orderBy("level", "manager_id")
    .limit(10)
)
