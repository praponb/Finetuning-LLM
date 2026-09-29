from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.location").isin(["Singapore", "Chiang Mai", "Berlin"]) & F.col("e.dept_id").isin([2, 31, 34]))
    .select(
        F.col("e.id"),
        F.col("e.email"),
        F.col("e.full_name"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.lower(F.col("e.level")).alias("level_lower"),
    )
)
