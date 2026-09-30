from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy("budget")
w2 = Window.orderBy(F.col("budget").desc_nulls_last())
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("dept_name").orderBy("budget_cume_dist")
w_ordered = Window.orderBy("budget")

kept_keys = (
    df_departments
    .select("location", "id")
    .intersect(
        df_departments
        .filter(F.col("location").isin(["Berlin", "Chiang Mai", "Bangkok"]))
        .select("location", "id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("budget_cume_dist"),
        F.percent_rank().over(w2).alias("budget_percent_rank"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isin([9, 15])), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("location_avg_budget", F.avg("budget").over(w3))
    .withColumn("budget_cume_dist_dense_rank", F.dense_rank().over(w4))
    .withColumn("budget_percent_rank_2", F.percent_rank().over(w_ordered))
)

result = (
    metrics
    .groupBy(
        "location",
        F.when(F.col("budget_percent_rank_2") >= 0.65, "LARGE").otherwise("SMALL").alias("budget_percent_rank_2_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
        F.min("budget_percent_rank_2").alias("min_budget_percent_rank_2"),
        F.round(F.sum("budget_percent_rank_2") / F.nullif(F.count(F.when(F.col("dept_name") == "Sales", 1)), F.lit(0)), 2).alias("budget_percent_rank_2_per_sales"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events_2"),
    )
    .filter(F.col("new_york_budget") < 0)
)
