from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_last(), "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget_filled").desc())
w3 = Window.partitionBy("location").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("budget_filled", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("running_avg_budget_filled", "id")
w_part = Window.orderBy("budget_filled", "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_level", F.when(F.col("budget") >= 3649000, "High").when(F.col("budget") >= 1725000, "Medium").otherwise("Low"))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .filter(F.col("budget") < 3983000)
    .select(
        "id",
        "budget_filled",
        F.sum("budget_filled").over(w3).alias("running_sum_budget_filled"),
        F.avg("budget_filled").over(w3).alias("running_avg_budget_filled"),
        F.count("budget_filled").over(w3).alias("running_count_budget_filled"),
    )
)

with_windows = (
    metrics
    .withColumn("budget_filled_avg_2", F.avg("budget_filled").over(w_recent))
    .withColumn("next_running_count_budget_filled", F.lead("running_count_budget_filled", 1).over(w5))
    .withColumn("next_running_avg_budget_filled", F.lead("running_avg_budget_filled", 1).over(w_part))
)

result = (
    with_windows
    .select(
        "id",
        "running_avg_budget_filled",
        "running_count_budget_filled",
        "budget_filled_avg_2",
    )
)
