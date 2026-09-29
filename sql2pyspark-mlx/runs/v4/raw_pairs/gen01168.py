from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("dept_name", "location")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id")
w4 = Window.orderBy("budget_percent_rank")
w5 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w6 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget_percent_rank").desc(), "id")
w7 = Window.orderBy(F.col("budget_percent_rank").desc_nulls_first())
w8 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget_percent_rank_bucket").desc())

base = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "New York", "Tokyo"]))
    .select(
        "*",
        F.trim("dept_name").alias("clean_dept_name"),
        F.substring("dept_name", 1, 3).alias("dept_name_prefix"),
    )
)

metrics = (
    base
    .withColumn("budget_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
)

with_windows = (
    metrics
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.cume_dist().over(w4).alias("budget_percent_rank_cume_dist"),
        F.ntile(3).over(w5).alias("budget_bucket"),
        F.ntile(3).over(w6).alias("budget_percent_rank_bucket"),
        F.cume_dist().over(w7).alias("budget_percent_rank_cume_dist_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        (F.col("budget_share") - F.col("budget_percent_rank_bucket")).alias("budget_share_minus_budget_percent_rank_bucket"),
        (F.col("budget_percent_rank_cume_dist_2") - F.col("budget_bucket")).alias("budget_percent_rank_cume_dist_2_minus_budget_bucket"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level") == "L4"), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy("location")
    .agg(
        F.countDistinct("location").alias("distinct_locations"),
        F.round(F.sum("budget_percent_rank_cume_dist") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("budget_percent_rank_cume_dist_per_tokyo"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.sum("budget_share") / F.nullif(F.count(F.when(F.col("dept_name") == "Legal", 1)), F.lit(0)), 2).alias("budget_share_per_legal"),
    )
    .filter(F.col("budget_share_per_legal") > 0)
    .orderBy(F.col("distinct_locations").desc_nulls_first())
)
