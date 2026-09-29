from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") | (F.col("level") != "L5"))
    .select(
        "level",
        "full_name",
        "dept_id",
        F.upper("level").alias("level_upper"),
        F.concat_ws("-", "full_name", "level").alias("full_name_level_key"),
    )
)
