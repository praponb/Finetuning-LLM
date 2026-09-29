from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget_adjusted").desc_nulls_last(), "id")
w_ordered = Window.partitionBy("location").orderBy("budget", "id")
w3 = Window.orderBy(F.col("budget_adjusted").desc_nulls_first())
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("next_budget_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("*", F.round((F.col("budget") * 0.9), 2).alias("budget_adjusted"))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    deduped
    .withColumn("prev_budget", F.lag("budget").over(w_ordered))
    .withColumn("next_budget_adjusted", F.lead("budget_adjusted").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("budget_adjusted_rank", F.rank().over(w3))
)

enriched_departments = (
    enriched
    .withColumn("last_known_budget", F.last("budget", ignorenulls=True).over(w4))
    .withColumn("running_budget", F.sum("budget").over(w5))
)

cleaned = (
    enriched_departments
    .filter(F.col("next_budget_adjusted").between(36, 58))
    .select(
        "*",
        F.round("budget", 2).alias("budget_rounded"),
        (F.col("budget_adjusted_rank") + F.col("running_budget")).alias("budget_adjusted_rank_plus_running_budget"),
        F.coalesce("location", F.lit("OTHER")).alias("location_filled"),
    )
)

stats = (
    cleaned
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("next_budget_adjusted")).otherwise(0)).alias("finance_next_budget_adjusted"),
    )
    .filter(F.col("finance_next_budget_adjusted") >= 0)
)

result = stats.select("seq_no", "finance_next_budget_adjusted")
