from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("plan").orderBy("signup_date", "id")
w_row_count_distinct_plans_by = Window.partitionBy("row_count", "distinct_plans").orderBy("row_count", "plan")
w_distinct_plans_row_count = Window.partitionBy("distinct_plans", "row_count")
w_plan_row_count_by_avg_birth = Window.partitionBy("plan", "row_count").orderBy(F.col("avg_birth_year").asc_nulls_last())

filtered = (
    df_listeners
    .filter((F.array_contains(F.col("interests"), "jazz")) | (F.length(F.trim(F.col("username"))) >= 5))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

grouped = (
    latest
    .groupBy("plan")
    .agg(
        F.round(F.avg("birth_year"), 2).alias("avg_birth_year"),
        F.round(F.stddev("birth_year"), 2).alias("stddev_birth_year"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

with_trends = (
    grouped
    .select(
        "plan",
        "avg_birth_year",
        "stddev_birth_year",
        "distinct_plans",
        "row_count",
        F.lead("distinct_plans", 3, 0).over(w_row_count_distinct_plans_by).alias("next3_distinct_plans"),
        F.round(F.col("avg_birth_year") / F.nullif(F.sum("avg_birth_year").over(w_distinct_plans_row_count), F.lit(0)), 4).alias("avg_birth_year_share_of_distinct_plans_row_count"),
        F.dense_rank().over(w_plan_row_count_by_avg_birth).alias("avg_birth_year_dense_rank"),
        F.round(F.avg("row_count").over(w_distinct_plans_row_count), 2).alias("distinct_plans_row_count_avg_row_count"),
        F.round(F.col("stddev_birth_year") / F.nullif(F.sum("stddev_birth_year").over(w_distinct_plans_row_count), F.lit(0)), 4).alias("stddev_birth_year_share_of_distinct_plans_row_co"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("avg_birth_year_dense_rank") <= 3)
    .drop("avg_birth_year_dense_rank")
)

annotated = (
    top_rows
    .filter(F.col("row_count") < 9)
    .select(
        "*",
        (F.col("row_count") - F.col("stddev_birth_year")).alias("row_count_minus_stddev_birth_year"),
        F.concat(F.col("plan"), F.lit("#"), F.col("distinct_plans").cast("string")).alias("plan_ref"),
    )
)

result = (
    annotated
    .groupingSets(
        [["plan", "distinct_plans"], ["plan"], []],
        "plan", "distinct_plans",
    )
    .agg(
        F.sum("row_count").alias("total_row_count"),
        F.max("row_count_minus_stddev_birth_year").alias("max_row_count_minus_stddev_birth_year"),
        F.max("stddev_birth_year_share_of_distinct_plans_row_co").alias("max_stddev_birth_year_share_of_distinct_plans_ro"),
    )
)
