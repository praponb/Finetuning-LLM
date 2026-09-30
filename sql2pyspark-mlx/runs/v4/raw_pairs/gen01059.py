from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.dept_name"), F.col("d.location"))
    .agg(
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.countDistinct(F.col("e.salary")).alias("unique_depts"),
    )
    .filter(F.col("salary_range") > 24)
)
