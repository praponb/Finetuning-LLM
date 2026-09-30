from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter((F.col("d.budget") > 2853000) & ~F.col("d.location").isin(["Bangkok", "Chiang Mai", "Berlin"]))
    .select(F.col("e.full_name"), F.col("e.job_title"), F.col("d.location"))
)
