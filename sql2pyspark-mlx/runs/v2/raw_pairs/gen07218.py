from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Chiang Mai", "New York"]))
    .groupBy("location")
    .pivot("dept_name", ["Finance", "Sales", "HR", "Marketing"])
    .agg(F.avg("budget"))
    .orderBy("location")
)
