from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "HR"]) & (F.col("dept_name") == "Engineering"))
    .select(
        "id",
        "dept_name",
        "location",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
    )
)
