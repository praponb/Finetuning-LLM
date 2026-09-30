from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.when(F.col("dept_name") == "HR", 1).otherwise(0).alias("is_hr"),
    )
)
