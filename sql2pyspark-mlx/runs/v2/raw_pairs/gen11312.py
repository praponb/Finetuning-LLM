from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1105000)
    .select("location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isin(["Sales", "Engineering", "Marketing"]))
        .select("location", "dept_name")
    )
    .orderBy("dept_name")
)
