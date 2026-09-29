from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3162000, "high").when(F.col("budget") >= 860000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") >= 2009000)
    .select("budget_bucket", "max_budget")
    .orderBy("budget_bucket")
)
