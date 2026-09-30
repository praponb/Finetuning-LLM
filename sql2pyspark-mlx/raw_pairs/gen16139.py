from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name").orderBy("hire_date", "id")
w_ordered = Window.partitionBy(F.month("hire_date")).orderBy(F.col("budget").desc_nulls_first())
w_part = Window.partitionBy("dept_name", "location").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("dept_name", "location").orderBy("budget_avg_3", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("dept_name")
w7 = Window.partitionBy("location")
w8 = Window.partitionBy("dept_name").orderBy("last7_min_budget_avg_3", "id").rowsBetween(-3, -1)

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") >= F.col("s.hire_date")), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        F.row_number().over(w2).alias("event_seq"),
        F.dense_rank().over(w_ordered).alias("budget_dense_rank"),
        F.avg("budget").over(w_part).alias("budget_avg_3"),
    )
)

metrics = (
    enriched
    .filter(F.col("budget_avg_3") > 1923327)
    .withColumn("last7_min_budget_avg_3", F.min("budget_avg_3").over(w_running))
    .withColumn("last7_avg_budget_avg_3", F.avg("budget_avg_3").over(w_running))
    .withColumn("last7_max_budget_avg_3", F.max("budget_avg_3").over(w_running))
    .withColumn("dept_name_min_budget_avg_3", F.min("budget_avg_3").over(w6))
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id") == 8), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.round((F.col("last7_max_budget_avg_3") / F.sum("last7_max_budget_avg_3").over(w7)), 4).alias("last7_max_budget_avg_3_share"),
        F.avg("last7_avg_budget_avg_3").over(w8).alias("last7_avg_budget_avg_3_avg_3_prior"),
    )
)

result = (
    scored
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
    )
    .filter(F.col("marketing_count") > 1)
    .orderBy(F.col("hr_count").asc_nulls_first())
)
