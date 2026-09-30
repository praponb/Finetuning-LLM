from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "dept_id",
        F.concat_ws("-", "level", "full_name").alias("level_full_name_key"),
    )
)
