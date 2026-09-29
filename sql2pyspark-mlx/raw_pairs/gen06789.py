from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("dept_id").isin([5, 20, 29]) & F.col("is_remote"))
    .select(
        "id",
        "email",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
