from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3292000)
    .select("location", "dept_name")
    .unionByName(
        df_departments
        .filter(~F.col("dept_name").isin(["Marketing", "Engineering", "Legal"]))
        .select("location", "dept_name")
    )
    .distinct()
    .orderBy("dept_name")
)
