from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.countDistinct("salary").alias("unique_levels"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.l3_count") > 4)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.l3_count"),
        F.col("ds.unique_levels"),
    )
    .orderBy(F.col("l3_count").desc())
)
