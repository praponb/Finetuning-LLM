from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1127000)
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.when(F.col("dept_name").isin(["Sales", "Finance"]), "Group A").otherwise("Other").alias("dept_name_group"),
    )
)
