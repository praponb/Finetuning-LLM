from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.when(F.col("dept_name").isin(["Finance", "Marketing"]), "Priority").otherwise("Other").alias("dept_name_group"),
    )
    .orderBy(F.col("dept_name_group").desc())
)
