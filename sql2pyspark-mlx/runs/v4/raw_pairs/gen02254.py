from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("segment", "country")
w3 = Window.partitionBy("city", "country").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("segment").orderBy(F.col("age_avg_3").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

latest = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.avg("age").over(w3).alias("age_avg_3"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "id",
        "email",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "rank_in_group",
        "age_avg_3",
        "credit_limit_share_2",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

latest_customers = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

with_latest = (
    latest_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.plan") == "basic"), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"), F.col("c.amount").alias("order_amount"))
)

result = (
    with_last
    .groupBy("segment")
    .agg(
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.round(F.avg("age_avg_3"), 2).alias("avg_age_avg_3"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.min("credit_limit_share_2").alias("min_credit_limit_share_2"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
    )
)
