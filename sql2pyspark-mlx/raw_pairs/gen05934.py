from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("is_finance").desc(), "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location").orderBy("is_finance", "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("is_finance").asc_nulls_first())
w5 = Window.partitionBy("dept_name", "location").orderBy("is_finance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("is_finance", "id")
w7 = Window.partitionBy("location", "dept_name").orderBy(F.col("location_max_is_finance").desc_nulls_last(), "id")
w_ordered = Window.orderBy("is_finance_bucket_2", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.orderBy(F.col("is_finance_bucket_2").desc_nulls_first())
w10 = Window.orderBy(F.col("last3_min_last_known_is_finance_bucket_2").desc_nulls_last())
w_part = Window.orderBy(F.col("last3_min_last_known_is_finance_bucket_2").desc())
w_part_2 = Window.orderBy(F.col("last3_count_last_known_is_finance_bucket_2").desc_nulls_last())

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("budget") >= 1984000))
    .withColumn("is_finance", F.when(F.col("dept_name") == "Finance", 1).otherwise(0))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

with_windows = (
    valid
    .filter(F.col("dept_name").isin(["Marketing", "Engineering"]))
    .select(
        "*",
        F.ntile(3).over(w_recent).alias("is_finance_bucket"),
        F.max("is_finance").over(w2).alias("location_max_is_finance"),
        F.ntile(5).over(w3).alias("is_finance_bucket_2"),
        F.rank().over(w4).alias("is_finance_rank"),
    )
)

enriched = (
    with_windows
    .withColumn("last_known_is_finance_bucket_2", F.last("is_finance_bucket_2", ignorenulls=True).over(w5))
    .withColumn("is_finance_bucket_3", F.ntile(10).over(w6))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "is_finance_bucket",
        "is_finance_bucket_2",
        "is_finance_rank",
        "last_known_is_finance_bucket_2",
    )
)

metrics = (
    latest
    .select(
        "id",
        "is_finance_bucket",
        "is_finance_bucket_2",
        "is_finance_rank",
        F.min("last_known_is_finance_bucket_2").over(w_ordered).alias("last3_min_last_known_is_finance_bucket_2"),
        F.count("last_known_is_finance_bucket_2").over(w_ordered).alias("last3_count_last_known_is_finance_bucket_2"),
        F.max("last_known_is_finance_bucket_2").over(w_ordered).alias("last3_max_last_known_is_finance_bucket_2"),
        F.avg("last_known_is_finance_bucket_2").over(w_ordered).alias("last3_avg_last_known_is_finance_bucket_2"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .intersect(
        df_departments
        .filter(F.col("budget").between(3406000, 3759000))
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "id",
        "is_finance_bucket_2",
        "is_finance_rank",
        "last3_min_last_known_is_finance_bucket_2",
        "last3_count_last_known_is_finance_bucket_2",
        "last3_max_last_known_is_finance_bucket_2",
        F.percent_rank().over(w_running).alias("is_finance_bucket_2_percent_rank"),
        F.dense_rank().over(w10).alias("last3_min_last_known_is_finance_bucket_2_dense_rank"),
        F.cume_dist().over(w_part).alias("last3_min_last_known_is_finance_bucket_2_cume_dist"),
        F.percent_rank().over(w_part_2).alias("last3_count_last_known_is_finance_bucket_2_percent_rank"),
    )
)

stats = (
    scored
    .groupBy(
        F.when(F.col("last3_min_last_known_is_finance_bucket_2_cume_dist") >= 0.68, "LARGE").otherwise("SMALL").alias("last3_min_last_known_is_finance_bucket_2_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("is_finance_rank"), 2).alias("avg_is_finance_rank"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last3_max_last_known_is_finance_bucket_2", 0.95).alias("last3_max_last_known_is_finance_bucket_2_p95"),
    )
)

result = (
    stats
    .select(
        "last3_min_last_known_is_finance_bucket_2_cume_dist_tier",
        "avg_is_finance_rank",
        "event_count",
        "distinct_ids",
        "last3_max_last_known_is_finance_bucket_2_p95",
    )
    .orderBy(
        F.col("distinct_ids").desc_nulls_first(),
        "last3_max_last_known_is_finance_bucket_2_p95",
    )
)
