from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3313000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isin(["Legal", "Finance"]))
        .select("dept_name", "location")
    )
    .orderBy("location")
)
