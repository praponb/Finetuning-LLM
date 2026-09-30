from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.avg("salary").alias("avg_salary"),
    )
    .orderBy("avg_salary")
)
