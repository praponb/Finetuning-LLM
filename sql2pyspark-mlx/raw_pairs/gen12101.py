from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .select(
        F.col("d.budget"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
        F.col("e.hire_date"),
        F.when(F.col("d.location").isin(["Chiang Mai", "London"]), "Tier 1").otherwise("Other").alias("location_group"),
    )
)
