from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        F.when(F.col("level").isin(["L2", "L4"]), "Tier 1").otherwise("Other").alias("level_group"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
