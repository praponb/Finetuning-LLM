from pyspark.sql import functions as F

result = (
    df_sessions
    .select(F.col("device").alias("label"))
    .unionByName(df_employees.select(F.col("email").alias("label")))
    .orderBy("label")
)
