from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.location").isin(["Chiang Mai", "Bangkok"]))
    .groupBy(F.col("d.location"))
    .agg(
        F.sum(F.when(F.col("e.job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
    )
)
