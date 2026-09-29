from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") | (F.col("level") == "L3"))
    .select(
        "id",
        "manager_id",
        "hire_date",
        "level",
        F.round("salary", 1).alias("salary_rounded"),
    )
)
