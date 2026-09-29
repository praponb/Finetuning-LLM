from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Bangkok"]))
    .select(F.col("city").alias("label"))
    .unionByName(df_departments.select(F.col("location").alias("label")))
)
