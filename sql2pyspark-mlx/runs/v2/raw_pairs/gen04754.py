from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 2266000) & ((F.col("budget") <= 2113000) | F.col("dept_name").isin(["Engineering", "Marketing", "HR"])))
    .orderBy(F.col("id").desc(), "location")
)
