from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Finance"]))
    .select("location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location").isNull())
        .select("location", "dept_name")
    )
    .distinct()
)
