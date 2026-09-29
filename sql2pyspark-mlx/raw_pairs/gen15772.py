from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.col("dept_id") == 7) & (~F.col("is_remote") | F.col("dept_id").isin([6, 8, 14, 36]))))
    .select(
        "id",
        "hire_date",
        F.concat_ws("-", "full_name", "email").alias("full_name_email_key"),
    )
)
