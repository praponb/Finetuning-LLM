from pyspark.sql import functions as F

base = df_departments.filter(F.col("location") == "Tokyo")

agg = (
    base
    .groupBy(
        F.when(F.col("dept_name").isin(["Sales", "Engineering"]), "group_a").otherwise("other").alias("dept_name_group"),
        F.when(F.col("budget") >= 4255000, "high").when(F.col("budget") >= 1174000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)

result = (
    agg
    .select("dept_name_group", "stddev_budget")
    .orderBy("dept_name_group")
)
