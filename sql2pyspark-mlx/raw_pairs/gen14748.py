from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("adjusted_monthly_fee").desc())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w5 = Window.partitionBy("plan").orderBy("end_date", "id")
w6 = Window.partitionBy("status").orderBy("first_known_adjusted_monthly_fee", "id")
w7 = Window.partitionBy("plan").orderBy("prev_monthly_fee_2", "id")
w8 = Window.partitionBy("status").orderBy("prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("plan").orderBy(F.col("first_known_adjusted_monthly_fee").desc_nulls_first())

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-05-23"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round((F.col("monthly_fee") * 1.1), 2).alias("adjusted_monthly_fee"),
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "plan",
        "status",
        "adjusted_monthly_fee",
        F.dense_rank().over(w).alias("adjusted_monthly_fee_dense_rank"),
        F.stddev_samp("adjusted_monthly_fee").over(w2).alias("customer_id_stddev_samp_adjusted_monthly_fee"),
        F.first("adjusted_monthly_fee", ignorenulls=True).over(w3).alias("first_known_adjusted_monthly_fee"),
        F.lag("monthly_fee", 1).over(w4).alias("prev_monthly_fee"),
        F.lag("monthly_fee", 1).over(w5).alias("prev_monthly_fee_2"),
    )
)

enriched = (
    scored
    .withColumn("first_known_adjusted_monthly_fee_bucket", F.ntile(5).over(w6))
    .withColumn("prev_prev_monthly_fee_2", F.lag("prev_monthly_fee_2", 1, 0).over(w7))
    .withColumn("cumulative_first_known_adjusted_monthly_fee", F.sum("first_known_adjusted_monthly_fee").over(w8))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w9))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "plan",
        "status",
        "adjusted_monthly_fee",
        "adjusted_monthly_fee_dense_rank",
        "customer_id_stddev_samp_adjusted_monthly_fee",
        "first_known_adjusted_monthly_fee",
        "prev_monthly_fee",
        "prev_prev_monthly_fee_2",
    )
)

result = (
    deduped
    .cube("status")
    .agg(
        F.round(F.sum("prev_prev_monthly_fee_2") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("prev_prev_monthly_fee_2_per_basic"),
        F.count("*").alias("row_count"),
        F.grouping("status").alias("grouping_level"),
    )
    .orderBy(F.col("prev_prev_monthly_fee_2_per_basic").desc())
)
