from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "London", "Chiang Mai"]) & (F.col("budget").between(1731000, 2400000) | F.col("dept_name").isin(["HR", "Engineering"])))
    .groupBy(
        F.when(F.col("budget") >= 3809000, "high").when(F.col("budget") >= 1394000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_locations"))
    .orderBy("budget_bucket")
    .limit(3)
)
