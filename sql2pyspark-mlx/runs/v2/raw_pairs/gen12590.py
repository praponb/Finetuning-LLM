from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.length(F.col("e.email")) < 5)
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("e.salary"),
        F.when(F.col("d.location") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
    )
)
