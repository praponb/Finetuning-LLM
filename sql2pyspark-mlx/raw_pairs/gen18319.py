from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["HR", "Engineering", "Marketing"])
    .agg(F.max("budget"))
    .orderBy("location")
)
