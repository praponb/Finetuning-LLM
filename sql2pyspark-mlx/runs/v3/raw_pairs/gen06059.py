from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("location") == "Singapore") & F.col("location").isin(["Berlin", "Bangkok"])) | F.col("budget").isNotNull())
    .groupBy(
        F.when(F.col("budget") >= 3485000, "high").when(F.col("budget") >= 1017000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
        F.when(F.col("dept_name").isin(["Sales", "Engineering"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.sum("budget").alias("total_budget"))
)
