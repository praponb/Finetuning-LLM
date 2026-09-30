from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Singapore"]))
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Marketing"])
    .agg(F.max("budget"))
    .orderBy("location")
)
