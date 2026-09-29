from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Singapore") & F.col("location").isin(["London", "New York", "Berlin"]) & (F.col("budget") > 3025000))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 1.15), 2))
    .orderBy(F.col("budget_adjusted").desc())
)
