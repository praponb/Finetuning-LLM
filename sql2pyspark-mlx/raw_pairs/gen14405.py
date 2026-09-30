from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 30)
    .select(
        "id",
        "salary",
        "hire_date",
        F.substring("full_name", 1, 2).alias("full_name_prefix"),
    )
)
