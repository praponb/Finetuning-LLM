from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter((F.col("location") != "New York") & (F.col("budget") >= 1292000) & F.col("budget").between(1576000, 2706000))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
)

dept_name_summary = (
    recent_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
    )
)

result = (
    dept_name_summary
    .filter(F.col("legal_count") > 79)
    .orderBy("dept_name")
)
