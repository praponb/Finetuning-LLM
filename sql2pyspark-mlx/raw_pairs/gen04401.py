from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.lower(F.trim("email")) == "ivy@gmail.com")
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("total_count"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .filter(F.col("D.total_count") >= 7)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("D.total_count"),
        F.col("D.total_salary"),
    )
    .orderBy(F.col("total_count").desc(), "location")
)
