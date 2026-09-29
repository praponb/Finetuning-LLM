from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", F.ceil("budget").alias("budget_whole"))
    .orderBy("budget_whole")
)
