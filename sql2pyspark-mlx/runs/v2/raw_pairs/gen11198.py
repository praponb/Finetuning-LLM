from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2847000)
    .select(
        "id",
        "location",
        "budget",
        F.substring("dept_name", 1, 4).alias("dept_name_prefix"),
    )
)
