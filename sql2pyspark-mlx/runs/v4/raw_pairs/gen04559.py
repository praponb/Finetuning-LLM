from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & (F.col("dept_name").isin(["HR", "Marketing"]) | F.col("location").isNull()))
    .withColumn("location_group", F.when(F.col("location").isin(["London", "New York"]), "Group A").otherwise("Other"))
)

dept_name_summary = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("location") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
)

result = (
    dept_name_summary
    .filter(F.col("new_york_count") < 31)
    .orderBy("dept_name")
)
