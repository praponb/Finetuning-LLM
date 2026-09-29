from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNull() & F.col("location").isin(["Tokyo", "Bangkok"]))
    .groupBy("location")
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)
