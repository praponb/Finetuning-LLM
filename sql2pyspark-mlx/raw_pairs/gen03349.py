from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_first())
w2 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("location").orderBy("next_budget_2")
w6 = Window.partitionBy("dept_name").orderBy("next_budget_2", "id")
w7 = Window.partitionBy("dept_name", "location").orderBy("next_budget", "id")
w8 = Window.orderBy("cumulative_budget", "id")
w_recent = Window.partitionBy("location").orderBy("next_budget_rank", "id")
w_grp = Window.partitionBy("location").orderBy(F.col("budget_avg_3_prior").asc_nulls_first())
w11 = Window.partitionBy("location").orderBy(F.col("prev_budget").desc(), "id")

with_windows = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .select(
        "*",
        F.rank().over(w).alias("budget_rank"),
        F.sum("budget").over(w2).alias("cumulative_budget"),
        F.lead("budget", 2).over(w3).alias("next_budget"),
        F.avg("budget").over(w4).alias("budget_avg_3_prior"),
        F.lead("budget", 2).over(w3).alias("next_budget_2"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    deduped
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .select(
        "id",
        "location",
        "next_budget",
        "budget_avg_3_prior",
        "next_budget_2",
        "dept_name_lower",
        "resolved_dept_name",
        F.lead("budget_rank").over(w7).alias("next_budget_rank"),
        F.lag("budget").over(w7).alias("prev_budget"),
        F.row_number().over(w7).alias("row_seq"),
        F.ntile(10).over(w8).alias("cumulative_budget_bucket"),
    )
)

metrics = (
    enriched
    .withColumn("next_next_budget", F.lead("next_budget", 2).over(w_recent))
    .withColumn("budget_avg_3_prior_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("event_seq", F.row_number().over(w11))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.lower(F.col("c.email")).like("%any%")), "left_anti")
)

result = (
    not_flagged
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("row_seq")).otherwise(0)).alias("london_row_seq"),
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget_avg_3_prior")).otherwise(0)).alias("berlin_budget_avg_3_prior"),
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget_avg_3_prior")).otherwise(0)).alias("tokyo_budget_avg_3_prior"),
    )
    .filter(F.col("tokyo_budget_avg_3_prior") > 0)
)
