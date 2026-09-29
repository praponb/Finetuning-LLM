from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(~F.col("location").isin(["Bangkok", "New York", "London"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.full_name").like("bo%"))
    .select(
        F.col("e.id"),
        F.col("e.full_name"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
    )
)
