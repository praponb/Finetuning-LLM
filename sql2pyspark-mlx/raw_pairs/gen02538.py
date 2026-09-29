from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Engineering")
    .select("location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("budget") <= 2622000)
        .select("location", "dept_name")
    )
    .orderBy("location")
)
