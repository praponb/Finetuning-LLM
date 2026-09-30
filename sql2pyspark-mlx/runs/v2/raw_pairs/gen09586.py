from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "outer")
    .filter(F.col("d.budget") <= 1455000)
    .groupBy(F.col("d.dept_name"), F.col("d.location"))
    .agg(F.round(F.stddev(F.col("e.salary")), 2).alias("stddev_salary"))
    .filter(F.col("stddev_salary") > 28)
    .orderBy("stddev_salary", "dept_name")
)
