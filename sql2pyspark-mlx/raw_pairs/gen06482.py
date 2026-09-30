from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok"]))
    .groupBy(
        F.when(F.col("location").isin(["Bangkok", "New York"]), "group_a").otherwise("other").alias("location_group"),
        F.when(F.col("budget") >= 3213000, "high").when(F.col("budget") >= 958000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
    )
    .filter(F.col("bangkok_budget") > 50)
)
