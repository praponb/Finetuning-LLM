from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "hire_date",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
