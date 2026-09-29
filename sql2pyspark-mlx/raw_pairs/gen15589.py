from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "dept_name",
        F.when(F.col("dept_name") == "HR", 1).otherwise(0).alias("is_hr"),
        F.length("dept_name").alias("dept_name_length"),
    )
)
