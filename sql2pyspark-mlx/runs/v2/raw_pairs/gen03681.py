from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.dept_name"))
    .agg(
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
    )
)
