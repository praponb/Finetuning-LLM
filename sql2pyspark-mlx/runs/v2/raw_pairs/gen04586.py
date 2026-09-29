from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2513000, 3756000))
    .select("id", "budget", "location", F.ceil("budget").alias("budget_whole"))
)
