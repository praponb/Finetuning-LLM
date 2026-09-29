from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee_adjusted").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_adjusted").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy("monthly_fee_adjusted", "id")
w5 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("plan").orderBy("adjusted_monthly_fee_adjusted_dense_rank", "id")
w7 = Window.partitionBy("monthly_fee_adjusted_filled_tier").orderBy(F.col("basic_rows").desc())

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-05-05").cast("date")) & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.round((F.col("monthly_fee") * 0.85), 2).alias("monthly_fee_adjusted"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .withColumn("monthly_fee_adjusted_percent_rank", F.percent_rank().over(w2))
    .withColumn("monthly_fee_adjusted_dense_rank", F.dense_rank().over(w3))
    .withColumn("monthly_fee_adjusted_bucket", F.ntile(10).over(w4))
    .withColumn("running_monthly_fee_adjusted", F.sum("monthly_fee_adjusted").over(w5))
)

valid = (
    metrics
    .filter(F.col("status").isin(["EXPIRED", "PAUSED"]))
    .select(
        "*",
        F.round((F.col("monthly_fee_adjusted_dense_rank") * 100), 2).alias("adjusted_monthly_fee_adjusted_dense_rank"),
    )
)

base = (
    valid
    .filter((F.col("start_date") > F.lit("2023-09-08").cast("date")) & F.col("start_date").between("2023-10-01", "2025-08-01"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_adjusted",
        "rn",
        "monthly_fee_adjusted_percent_rank",
        "monthly_fee_adjusted_dense_rank",
        "running_monthly_fee_adjusted",
        "adjusted_monthly_fee_adjusted_dense_rank",
        F.coalesce("monthly_fee_adjusted", F.lit(0)).alias("monthly_fee_adjusted_filled"),
        (F.col("monthly_fee_adjusted_dense_rank") + F.col("monthly_fee_adjusted_bucket")).alias("monthly_fee_adjusted_dense_rank_plus_monthly_fee_adjusted_bucket"),
    )
)

latest_subscriptions = (
    base
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee_adjusted_filled") >= 69, "LARGE").otherwise("SMALL").alias("monthly_fee_adjusted_filled_tier"),
        F.when(F.col("monthly_fee_adjusted_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("monthly_fee_adjusted_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .filter(F.col("basic_rows") >= 1)
    .withColumn("basic_rows_rank", F.rank().over(w7))
    .orderBy("monthly_fee_adjusted_dense_rank_tier")
)
