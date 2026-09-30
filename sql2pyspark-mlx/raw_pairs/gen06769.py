from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy("budget", "id")
w3 = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name", "location").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last())
w6 = Window.partitionBy("dept_name").orderBy("dedup_rank", "id")
w7 = Window.orderBy(F.col("budget").desc_nulls_first())
w8 = Window.partitionBy("dept_name", "location").orderBy("prev_budget", "id")
w_hist = Window.partitionBy("location").orderBy("dedup_rank", "id").rowsBetween(-5, -1)
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w11 = Window.partitionBy("employee_level").orderBy(F.col("budget_cume_dist").desc(), "id")
w12 = Window.partitionBy("dept_name").orderBy(F.col("budget_percent_rank").desc(), "id")
w13 = Window.orderBy(F.col("budget_cume_dist").desc_nulls_last())

deduped = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .filter(~F.col("location").isin(["New York", "Tokyo", "Berlin"]))
    .withColumn("prev_budget", F.lag("budget", 1).over(w2))
    .withColumn("last_known_dept_name", F.last("dept_name", ignorenulls=True).over(w3))
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w4))
)

metrics_departments = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w5).alias("budget_percent_rank"),
        F.lead("budget", 2).over(w6).alias("next_budget"),
        F.cume_dist().over(w7).alias("budget_cume_dist"),
        F.ntile(3).over(w8).alias("prev_budget_bucket"),
        F.avg("budget").over(w_hist).alias("budget_avg_5_prior"),
    )
)

latest_match = (
    metrics_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.level").isin(["L1", "L5", "L4"])), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "location",
        "dedup_rank",
        "prev_budget",
        "last_known_dept_name",
        "next_budget",
        "prev_budget_bucket",
        F.ntile(4).over(w11).alias("budget_cume_dist_bucket"),
        F.row_number().over(w12).alias("seq_no"),
        F.ntile(10).over(w).alias("budget_bucket"),
        F.rank().over(w13).alias("budget_cume_dist_rank"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") < "2024-08-23"), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "location",
        "last_known_dept_name",
        "next_budget",
        "prev_budget_bucket",
        "budget_cume_dist_bucket",
        "seq_no",
        "budget_bucket",
        "budget_cume_dist_rank",
        F.when(F.col("location").rlike("^(Tokyo|New York)$"), "CORE").otherwise("GROUP_B").alias("location_zone"),
    )
    .orderBy(F.col("budget_cume_dist_bucket").desc())
)
