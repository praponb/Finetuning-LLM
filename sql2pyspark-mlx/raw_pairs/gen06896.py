from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Chiang Mai"]) & ((F.col("dept_name") != "Sales") | (F.col("budget") > 2208000)))
    .groupBy(
        F.when(F.col("budget") >= 4121000, "high").when(F.col("budget") >= 1350000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.count(F.when(F.col("location") == "London", 1)).alias("london_count"))
    .filter(F.col("london_count") < 1)
)
