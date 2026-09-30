from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("level").isin(["L1", "L2", "L4"]))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.stddev_salary") > 54)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.l3_count"),
        F.col("ds.stddev_salary"),
    )
    .orderBy(F.col("l3_count").desc())
)
