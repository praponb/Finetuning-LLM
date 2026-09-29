from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") < 2261000))
    .select("id", "location")
    .orderBy("id")
    .limit(10)
)
