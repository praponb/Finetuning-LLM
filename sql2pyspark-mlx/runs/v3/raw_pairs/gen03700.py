from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "location", F.col("budget").cast("int").alias("budget_int"))
    .orderBy(F.col("location").desc(), "budget_int")
)
