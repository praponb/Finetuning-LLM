from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_departments
    .withColumn("budget_avg_5_prior", F.avg("budget").over(w))
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w2))
)

result = (
    with_windows
    .groupBy(
        "dept_name",
        "location",
        F.when(F.col("budget_avg_5_prior") >= 4413928, "HIGH").otherwise("LOW").alias("budget_avg_5_prior_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.avg("first_known_budget").alias("avg_first_known_budget"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list_2"),
    )
    .orderBy(F.col("location").asc_nulls_last())
)
