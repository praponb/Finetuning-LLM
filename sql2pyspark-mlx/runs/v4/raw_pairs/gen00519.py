from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_last())
w_grp = Window.orderBy("location_length", "id")
w3 = Window.partitionBy("dept_name").orderBy(F.col("prev_budget").desc_nulls_last())
w4 = Window.partitionBy("location").orderBy("prev_budget", "id")
w5 = Window.partitionBy("location")
w6 = Window.orderBy(F.col("prev_budget").desc(), "id")
w7 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

base = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .withColumn("location_length", F.length("location"))
)

metrics = (
    base
    .select(
        "*",
        F.rank().over(w).alias("budget_rank"),
        F.lag("budget", 2).over(w_grp).alias("prev_budget"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w3).alias("prev_budget_dense_rank"),
        F.lead("budget", 2).over(w4).alias("next_budget"),
        F.sum("prev_budget").over(w5).alias("location_sum_prev_budget"),
        F.row_number().over(w6).alias("row_seq"),
        F.round((F.col("budget") / F.sum("budget").over(w5)), 4).alias("budget_share"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .cube("location")
    .agg(
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.min("location_sum_prev_budget").alias("min_location_sum_prev_budget"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
    )
    .filter(F.col("berlin_count") < 1)
)
