from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("dept_name").isNotNull())
    .select(
        "*",
        F.when(F.col("dept_name") == "Finance", 1).otherwise(0).alias("is_finance"),
    )
)

metrics = (
    base
    .withColumn("running_sum_budget", F.sum("budget").over(w))
    .withColumn("running_min_budget", F.min("budget").over(w))
    .withColumn("running_max_budget", F.max("budget").over(w))
    .withColumn("running_count_budget", F.count("budget").over(w))
)

cleaned = (
    metrics
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "*",
        F.round((F.col("running_max_budget") * 1.1), 2).alias("running_max_budget_adjusted"),
        (F.col("is_finance") + F.col("running_max_budget")).alias("is_finance_plus_running_max_budget"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.location", "s.running_count_budget", F.col("c.hire_date"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.hire_date")), "left_anti")
)

result = (
    not_flagged
    .withColumn("running_count_budget_tier", F.when(F.col("running_count_budget") >= 17, "HIGH").otherwise("LOW"))
    .rollup("running_count_budget_tier", "location")
    .agg(
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_rows"),
        F.round(F.sum("running_count_budget") / F.nullif(F.count(F.when(F.col("location") == "Chiang Mai", 1)), F.lit(0)), 2).alias("running_count_budget_per_chiang_mai"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.round(F.avg("running_count_budget"), 2).alias("avg_running_count_budget"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.grouping("running_count_budget_tier").alias("gid"),
    )
    .filter(F.col("num_locations") >= 1)
)
