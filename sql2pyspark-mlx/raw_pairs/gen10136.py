from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("d.location").isin(["New York", "Chiang Mai"]) & F.col("e.dept_id").isin([25, 27, 32, 34]))
    .select(
        F.col("e.id"),
        F.col("e.email"),
        F.col("e.is_remote"),
        F.col("e.manager_id"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)
