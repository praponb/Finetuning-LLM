from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Legal"]))
    .cube(
        "location",
        F.when(F.col("budget") >= 3071000, "high").when(F.col("budget") >= 1651000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
)
