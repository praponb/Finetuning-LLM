from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3714000, "high").when(F.col("budget") >= 891000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
)
