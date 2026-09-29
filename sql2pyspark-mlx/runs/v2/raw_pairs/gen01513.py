from pyspark.sql import functions as F

filtered_employees = df_employees.filter((F.col("hire_date") >= "2023-04-04") & (F.col("dept_id") == 24))

level_summary = (
    filtered_employees
    .groupBy("level")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    level_summary
    .filter(F.col("avg_salary") < 99000)
    .select("level", "level_values", "avg_salary")
)
