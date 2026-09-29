from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Legal"]))
    .groupBy("dept_name")
    .pivot("location", ["London", "Bangkok", "Berlin", "Singapore"])
    .agg(F.sum("budget"))
)
