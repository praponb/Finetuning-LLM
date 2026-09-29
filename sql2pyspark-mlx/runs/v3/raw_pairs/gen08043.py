from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.col("dept_id").isin([7, 9, 13, 25]))

level_summary = (
    filtered_employees
    .groupBy("level")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    level_summary
    .filter(F.col("avg_salary") < 67)
    .select("level", "avg_salary")
    .orderBy(F.col("avg_salary").desc(), "level")
)
