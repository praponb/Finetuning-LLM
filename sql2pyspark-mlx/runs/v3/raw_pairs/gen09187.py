from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(121000, 142000))
    .select(
        "salary",
        "dept_id",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.trim("full_name").alias("clean_full_name"),
    )
)
