from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.salary").between(109000, 146000))
    .groupBy(F.col("d.dept_name"))
    .agg(F.sum(F.col("e.salary")).alias("total_salary"))
    .filter(F.col("total_salary") >= 289000)
    .orderBy(F.col("total_salary").desc(), "dept_name")
    .limit(3)
)
