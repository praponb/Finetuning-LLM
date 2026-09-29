from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") >= 3204000)
        .select("dept_name", "location")
    )
    .orderBy(F.col("location").desc())
)
