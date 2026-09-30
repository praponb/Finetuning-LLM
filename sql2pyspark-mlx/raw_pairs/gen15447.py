from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("customer_id", "plan").orderBy(F.col("end_date").desc_nulls_first(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-05-14").cast("date"))
    .withColumn("monthly_fee_scaled", F.round((F.col("monthly_fee") * 100), 2))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked_subscriptions = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

filtered_subscriptions = (
    ranked_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-06-02").cast("date"))
    .withColumn("is_expired", F.when(F.col("status") == "EXPIRED", 1).otherwise(0))
)

with_dims = (
    filtered_subscriptions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.monthly_fee",
        "e.status",
        "e.rank_in_group",
        "e.row_num",
        "e.is_expired",
        F.col("c.segment"),
    )
)

result = (
    with_dims
    .withColumn("is_expired_tier", F.when(F.col("is_expired") >= 78, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["is_expired_tier", "segment"], ["is_expired_tier"]],
        "is_expired_tier",
        "segment",
    )
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.max(F.when(F.col("segment") == "Consumer", F.col("monthly_fee"))).alias("consumer_max_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.grouping("is_expired_tier").alias("grouping_level"),
    )
    .orderBy(F.col("segment").desc())
)
