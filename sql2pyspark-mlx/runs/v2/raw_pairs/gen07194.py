from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("id", "location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["HR", "Finance"]))
        .select("id", "location", "dept_name")
    )
)
