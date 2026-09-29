from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("dept_name").orderBy("budget_whole", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("location").orderBy("cumulative_budget_whole", "id")
w_grp = Window.partitionBy("location").orderBy("budget_whole_avg_3_prior", "id").rowsBetween(-3, -1)
w6 = Window.partitionBy("location").orderBy(F.col("budget_whole_avg_3_prior").desc_nulls_first())
w7 = Window.orderBy(F.col("budget_whole_avg_3_prior").desc(), "id")
w8 = Window.partitionBy("location").orderBy("budget_whole_avg_3_prior", "id")
w9 = Window.partitionBy("location").orderBy(F.col("budget_whole_avg_3_prior_avg_3_prior").desc_nulls_last(), "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("location").isin(["Berlin", "Bangkok"]))
    .select(
        "*",
        F.floor("budget").alias("budget_whole"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "location",
        F.sum("budget_whole").over(w).alias("cumulative_budget_whole"),
        F.avg("budget_whole").over(w2).alias("budget_whole_avg_2_prior"),
        F.avg("budget_whole").over(w3).alias("budget_whole_avg_3_prior"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "location", "budget_whole_avg_3_prior")
)

enriched = (
    deduped
    .select(
        "id",
        "location",
        F.avg("budget_whole_avg_3_prior").over(w_grp).alias("budget_whole_avg_3_prior_avg_3_prior"),
        F.rank().over(w6).alias("budget_whole_avg_3_prior_rank"),
        F.row_number().over(w7).alias("seq_no"),
        F.ntile(10).over(w8).alias("budget_whole_avg_3_prior_bucket"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "location",
        "budget_whole_avg_3_prior_avg_3_prior",
        "seq_no",
        "row_num",
        F.when(F.col("location").rlike("^(New York|Berlin)$"), "GROUP_A").otherwise("PRIORITY").alias("location_zone"),
    )
    .orderBy("location", "location_zone")
)
