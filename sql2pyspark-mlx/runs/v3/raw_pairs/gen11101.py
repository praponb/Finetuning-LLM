from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.budget") >= 2444000)
    .select(
        F.col("e.dept_id"),
        F.col("e.level"),
        F.col("e.full_name"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("d.budget"),
        F.trim(F.col("e.email")).alias("clean_email"),
    )
    .orderBy("dept_id")
)
