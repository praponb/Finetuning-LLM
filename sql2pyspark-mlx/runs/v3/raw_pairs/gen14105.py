from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.location").isin(["New York", "London", "Chiang Mai"]) | (F.col("d.budget") > 3528000))
    .select(
        F.col("d.budget"),
        F.col("e.level"),
        F.col("e.dept_id"),
        F.col("e.manager_id"),
    )
)
