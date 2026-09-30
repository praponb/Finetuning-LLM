from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Finance"]) | F.col("dept_name").isin(["Finance", "Sales"]))
    .withColumn("budget_whole", F.ceil("budget"))
)

agg = (
    filtered_departments
    .groupBy("location")
    .agg(
        F.round(F.avg("budget_whole"), 2).alias("avg_budget_whole"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)

result = agg
