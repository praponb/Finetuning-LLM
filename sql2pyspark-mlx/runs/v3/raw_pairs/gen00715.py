from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 1926000) & ((F.col("budget") <= 2276000) | F.col("dept_name").isin(["Legal", "Engineering", "Finance"])))
    .select("dept_name", F.length("location").alias("location_length"))
)
