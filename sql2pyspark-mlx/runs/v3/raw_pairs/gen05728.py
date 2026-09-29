from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(~((F.col("budget") >= 3858000) | (F.col("budget") <= 1205000)))
    .withColumn("budget_rounded", F.round("budget", 2))
)

agg = (
    recent_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("budget_rounded")).otherwise(0)).alias("london_budget_rounded"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
)

result = (
    agg
    .filter(F.col("london_budget_rounded") > 43)
    .select("location", "london_budget_rounded", "dept_name_list")
)
