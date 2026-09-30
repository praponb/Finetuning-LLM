from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2087000)
    .select("id", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location").isin(["New York", "Chiang Mai"]))
        .select("id", "dept_name")
    )
    .distinct()
    .orderBy("dept_name")
)
