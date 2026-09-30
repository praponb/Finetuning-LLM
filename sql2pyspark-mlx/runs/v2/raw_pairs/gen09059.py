from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Legal"]) & (F.col("budget") <= 1403000) & (F.col("location") == "London"))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 1.07), 2))
)

summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 4465000, "high").when(F.col("budget") >= 1924000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.max("budget").alias("max_budget"))
)

result = (
    summary
    .filter(F.col("max_budget") >= 2018000)
    .select("budget_bucket", "max_budget")
)
