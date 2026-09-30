from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com") & (F.col("dept_id").isin([12, 30]) | (F.col("salary") >= 64000)))
    .select(
        "id",
        "full_name",
        "hire_date",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy(F.col("id").desc())
)
