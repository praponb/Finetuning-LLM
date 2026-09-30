from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("budget") >= 2406000) & ((F.col("dept_name") == "HR") | (F.col("location") != "Bangkok")))
)

budget_bucket_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3514000, "high").when(F.col("budget") >= 1192000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    budget_bucket_summary
    .filter(F.col("cnt") > 8)
    .select("budget_bucket", "location", "cnt", "max_budget")
    .orderBy(F.col("location").desc())
)
