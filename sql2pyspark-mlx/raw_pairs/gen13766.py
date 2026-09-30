from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_last(), "id")
w2 = Window.partitionBy("dept_name", "location").orderBy("row_num", "id")
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w_ordered = Window.partitionBy("dept_name").orderBy("budget", "id")
w5 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_name").orderBy("prev_budget", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy(F.col("last4_avg_budget_bucket").asc_nulls_last())
w8 = Window.orderBy("prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("last4_sum_budget_bucket", "id")

latest = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id") == 14), "left_anti")
)

eligible_departments = (
    eligible
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") > "2023-12-10"), "left_semi")
)

scored = (
    eligible_departments
    .select(
        "id",
        "dept_name",
        F.lead("budget", 1).over(w2).alias("next_budget"),
        F.ntile(10).over(w3).alias("budget_bucket"),
        F.lag("budget", 1).over(w_ordered).alias("prev_budget"),
        F.sum("budget").over(w5).alias("cumulative_budget"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "next_budget",
        "budget_bucket",
        "prev_budget",
        F.sum("budget_bucket").over(w6).alias("last4_sum_budget_bucket"),
        F.avg("budget_bucket").over(w6).alias("last4_avg_budget_bucket"),
        F.min("budget_bucket").over(w6).alias("last4_min_budget_bucket"),
    )
)

enriched_departments = (
    enriched
    .select(
        "id",
        "next_budget",
        "budget_bucket",
        "last4_sum_budget_bucket",
        F.cume_dist().over(w7).alias("last4_avg_budget_bucket_cume_dist"),
        F.last("last4_sum_budget_bucket", ignorenulls=True).over(w8).alias("last_known_last4_sum_budget_bucket"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

not_flagged = (
    enriched_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.email").isNotNull()), "left_anti")
)

stats = (
    not_flagged
    .groupBy(
        F.when(F.col("last4_avg_budget_bucket_cume_dist") >= 0.86, "HIGH").otherwise("LOW").alias("last4_avg_budget_bucket_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("last_known_last4_sum_budget_bucket"), 2).alias("avg_last_known_last4_sum_budget_bucket"),
        F.percentile_approx("next_budget", 0.9).alias("next_budget_p90"),
    )
)

result = (
    stats
    .select("last4_avg_budget_bucket_cume_dist_tier", "next_budget_p90")
    .orderBy(
        F.col("next_budget_p90").desc_nulls_first(),
        F.col("last4_avg_budget_bucket_cume_dist_tier").desc(),
    )
)
