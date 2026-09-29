from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("dept_name").isin(["Legal", "Marketing"]) & (F.col("dept_name") == "Engineering")))
    .select(
        "location",
        F.when(F.col("dept_name").isin(["Legal", "Marketing"]), "Group A").otherwise("Other").alias("dept_name_group"),
        F.col("budget").cast("int").alias("budget_int"),
    )
)
