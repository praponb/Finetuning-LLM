from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2350000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("location").isin(["London", "Singapore", "Berlin"]))
        .select("dept_name", "location")
    )
    .distinct()
    .orderBy("location")
)
