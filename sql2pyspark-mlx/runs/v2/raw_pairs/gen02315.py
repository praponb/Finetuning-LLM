from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2598000)
    .select("id", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isin(["Legal", "HR"]))
        .select("id", "dept_name")
    )
)
