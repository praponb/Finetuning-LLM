from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("subscription_status").orderBy(F.col("subscription_end_date").desc_nulls_first(), "id")
w3 = Window.orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("age", "id")

with_last = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("subscription_status"),
        F.col("c.end_date").alias("subscription_end_date"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.subscription_end_date")), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.to_date(F.col("ch.started_at")) >= "2025-02-01"), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "email", "age")
)

kept_customers = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.discount") < 0.3), "left_anti")
)

not_flagged_customers = (
    kept_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.country").isin(["JP", "TH"])), "left_anti")
)

enriched = (
    not_flagged_customers
    .filter(F.col("email").like("%@company.co"))
    .withColumn("last3_max_age", F.max("age").over(w3))
    .withColumn("last3_count_age", F.count("age").over(w3))
    .withColumn("last3_avg_age", F.avg("age").over(w3))
    .withColumn("last3_sum_age", F.sum("age").over(w3))
    .withColumn("prev_age", F.lag("age", 1).over(w4))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("last3_avg_age") >= 73, "PREMIUM").otherwise("STANDARD").alias("last3_avg_age_tier"),
    )
    .agg(
        F.percentile_approx("prev_age", [0.25, 0.5, 0.75]).alias("prev_age_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("age").alias("age_bit_and"),
        F.round(F.avg("last3_max_age"), 2).alias("avg_last3_max_age"),
    )
    .orderBy(F.col("avg_last3_max_age").desc_nulls_last())
)
