from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1447000)
    .groupBy(
        "location",
        F.when(F.col("dept_name").isin(["Sales", "Finance"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"),
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
    .filter(F.col("new_york_count") > 4)
)
