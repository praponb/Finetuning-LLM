from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1731000)
    .select("dept_name")
    .intersect(
        df_departments
        .filter(F.col("location") != "Singapore")
        .select("dept_name")
    )
    .orderBy("dept_name")
)
