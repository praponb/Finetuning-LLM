from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2880000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("dept_name") == "Legal")
        .select("dept_name", "location")
    )
)
