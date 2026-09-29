from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Chiang Mai"]) & F.col("budget").between(3032000, 3706000))
)

departments_stats = (
    base
    .groupBy("location", "dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget")).otherwise(0)).alias("chiang_mai_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
)

result = (
    departments_stats
    .select("location", "dept_name", "chiang_mai_budget", "dept_name_list")
    .orderBy(F.col("chiang_mai_budget").desc(), "location", "dept_name")
    .limit(50)
)
