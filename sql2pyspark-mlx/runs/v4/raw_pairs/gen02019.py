from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 139000)
    .select(
        "id",
        "email",
        F.coalesce("level", F.lit("unassigned")).alias("level_filled"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)
