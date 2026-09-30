from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2752000, 3568000))
    .select("id", "dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Sales", "Engineering", "Finance"]))
        .select("id", "dept_name", "location")
    )
)
