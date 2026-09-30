from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Tokyo")
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4428000, "high").when(F.col("budget") >= 1155000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["Chiang Mai", "Bangkok"]), "group_a").otherwise("other").alias("location_group"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
    )
    .filter(F.col("legal_count") < 57)
    .orderBy(F.col("legal_count").desc(), F.col("dept_name").desc())
)
