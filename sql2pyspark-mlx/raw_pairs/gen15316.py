from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date"))
    .withColumn("salary_tier", F.when(F.col("salary") >= 128000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["activity_month", "salary_tier"], ["activity_month"]],
        "activity_month",
        "salary_tier",
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max(F.when(F.col("level") == "L4", F.col("salary"))).alias("l4_max_salary"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_rows"),
    )
)
