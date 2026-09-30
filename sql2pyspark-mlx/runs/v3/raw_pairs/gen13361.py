from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1197000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("budget") < 3222000)
        .select("dept_name", "location")
    )
    .orderBy("location")
)
