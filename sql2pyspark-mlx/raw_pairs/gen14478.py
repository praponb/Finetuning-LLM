from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") & ((F.col("budget") >= 2548000) | F.col("location").isin(["London", "Berlin", "Bangkok"])))
    .select("id", "location", F.lower("dept_name").alias("dept_name_lower"))
)
