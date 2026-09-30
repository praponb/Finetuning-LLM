from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location").isin(["London", "Bangkok", "Chiang Mai"]))
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("e.salary"),
        F.col("e.level"),
        F.col("e.dept_id"),
        F.upper(F.col("e.full_name")).alias("full_name_upper"),
    )
    .orderBy("salary")
)
