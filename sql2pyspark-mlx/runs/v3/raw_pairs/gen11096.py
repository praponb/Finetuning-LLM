from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 1580000) & (F.col("dept_name") != "Engineering") & (F.col("location") == "London"))
    .groupBy(
        F.when(F.col("budget") >= 3886000, "high").when(F.col("budget") >= 1809000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
)
