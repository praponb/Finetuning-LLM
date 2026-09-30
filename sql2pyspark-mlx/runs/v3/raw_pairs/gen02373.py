from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3253000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("location").isNotNull())
        .select("dept_name", "location")
    )
)
