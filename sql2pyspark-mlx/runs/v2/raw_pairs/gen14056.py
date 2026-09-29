from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1926000)
    .select("id", "location")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isin(["Engineering", "Sales"]))
        .select("id", "location")
    )
)
