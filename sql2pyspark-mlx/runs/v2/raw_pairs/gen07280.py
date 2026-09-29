from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Singapore"]) & F.col("dept_name").isin(["Finance", "Engineering", "Marketing"]))
    .select("location")
)
