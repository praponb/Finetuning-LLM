from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name") != "Engineering")
    .groupBy(F.col("d.location"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
    )
    .orderBy("pct_l4")
)
