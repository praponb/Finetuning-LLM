from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "London", "Bangkok"]) & F.col("dept_name").isin(["Legal", "Engineering", "Marketing"]) & (F.col("dept_name") == "Marketing"))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3822000, "high").when(F.col("budget") >= 819000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("unique_locations") >= 4)
)
