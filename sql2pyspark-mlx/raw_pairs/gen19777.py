from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")
w2 = Window.orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location", "dept_name").orderBy("latest_rank", "id")
w_running = Window.orderBy(F.col("budget").desc_nulls_first())
w_running_2 = Window.partitionBy("dept_name", "location")
w_ordered = Window.partitionBy("location", "dept_name").orderBy("budget_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("location", "dept_name").orderBy("dept_name_min_budget", "id")
w_recent = Window.partitionBy("dept_name").orderBy("prev_budget", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("location").orderBy(F.col("budget_bucket").desc(), "id")
w10 = Window.partitionBy("location").orderBy(F.col("prev_budget").desc(), "id")

ranked = (
    df_departments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .filter(F.col("location").isin(["Bangkok", "Singapore", "Chiang Mai"]))
    .withColumn("budget_bucket", F.ntile(3).over(w2))
    .withColumn("prev_budget", F.lag("budget", 1, 0).over(w3))
    .withColumn("budget_percent_rank", F.percent_rank().over(w_running))
    .withColumn("dept_name_min_budget", F.min("budget").over(w_running_2))
)

enriched_departments = (
    enriched
    .select(
        "id",
        "location",
        "budget",
        "latest_rank",
        "budget_bucket",
        "budget_percent_rank",
        "dept_name_min_budget",
        F.sum("budget").over(w_ordered).alias("cumulative_budget"),
        F.lead("budget_bucket", 1).over(w7).alias("next_budget_bucket"),
        F.avg("budget_percent_rank").over(w_recent).alias("budget_percent_rank_avg_6"),
        F.ntile(3).over(w9).alias("budget_bucket_bucket"),
        F.row_number().over(w10).alias("row_seq"),
    )
)

eligible = (
    enriched_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary").between(106000, 134000)), "left_anti")
)

result = (
    eligible
    .select("id", "location", "budget", "budget_percent_rank")
    .unpivot(
        ids=["id", "location"],
        values=["budget", "budget_percent_rank"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
