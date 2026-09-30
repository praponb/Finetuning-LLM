from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "manager_id",
        "full_name",
        F.initcap(F.trim("level")).alias("level_title"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy(F.col("full_name").desc())
)
