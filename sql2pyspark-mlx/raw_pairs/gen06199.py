from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.when(F.col("d.location").isin(["New York", "Tokyo"]), "core").otherwise("other").alias("location_group"),
    )
    .agg(F.sum(F.col("e.salary")).alias("total_salary"))
    .filter(F.col("total_salary") >= 475000)
    .orderBy(F.col("location_group").desc())
    .limit(5)
)
