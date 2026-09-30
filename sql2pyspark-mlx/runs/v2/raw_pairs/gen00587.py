from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("dept_id").isin([3, 6, 8, 31]))

employees_stats = (
    valid_employees
    .groupBy("level", "is_remote")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
)

result = (
    employees_stats
    .filter(F.col("l2_count") > 8)
    .select("level", "is_remote", "level_list", "l2_count")
)
