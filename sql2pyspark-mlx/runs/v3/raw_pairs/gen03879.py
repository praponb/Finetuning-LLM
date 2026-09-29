from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "budget",
        "dept_name",
        "location",
        F.when(F.col("dept_name").isin(["Sales", "Engineering"]), "Priority").otherwise("Other").alias("dept_name_group"),
    )
    .orderBy(F.col("dept_name_group").desc())
)
