from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3168000)
    .select("id", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isin(["Legal", "HR", "Finance"]))
        .select("id", "dept_name")
    )
    .orderBy("dept_name")
)
