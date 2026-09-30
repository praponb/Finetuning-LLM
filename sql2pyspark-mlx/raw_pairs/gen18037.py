from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") >= 9)
    .select(
        "id",
        "hire_date",
        "manager_id",
        "salary",
        F.round("salary", 2).alias("salary_rounded"),
        F.length("level").alias("level_length"),
    )
)
