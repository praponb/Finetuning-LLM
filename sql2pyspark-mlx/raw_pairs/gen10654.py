from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("age", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.orderBy("credit_limit_dense_rank", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("country", "city").orderBy("credit_limit", "id")
w5 = Window.partitionBy("segment").orderBy("last4_avg_age", "id")
w6 = Window.partitionBy("city").orderBy(F.col("new_york_avg_age").desc())

filtered = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "segment", "email").alias("segment_email_key"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

metrics = (
    filtered
    .withColumn("last4_min_age", F.min("age").over(w))
    .withColumn("last4_count_age", F.count("age").over(w))
    .withColumn("last4_avg_age", F.avg("age").over(w))
    .withColumn("last4_sum_age", F.sum("age").over(w))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w2))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") >= F.col("s.signup_date")), "left_anti")
)

scored = (
    eligible
    .filter(F.col("city").isin(["Singapore", "Chiang Mai", "Bangkok"]))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "is_active",
        "age",
        "segment_email_key",
        "last4_min_age",
        "last4_avg_age",
        "last4_sum_age",
        F.avg("age").over(w3).alias("age_avg_3"),
        F.lag("last4_avg_age", 2).over(w4).alias("prev_last4_avg_age"),
        F.lead("last4_min_age", 1).over(w5).alias("next_last4_min_age"),
    )
)

summary = (
    scored
    .groupBy(
        "city",
        "segment",
        F.when(F.col("last4_min_age") >= 58, "LARGE").otherwise("SMALL").alias("last4_min_age_tier"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.avg(F.when(F.col("city") == "New York", F.col("age"))).alias("new_york_avg_age"),
        F.avg(F.when(F.col("city") == "New York", F.col("age_avg_3"))).alias("new_york_avg_age_avg_3"),
        F.round(F.avg("age_avg_3"), 2).alias("avg_age_avg_3"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("new_york_avg_age_avg_3") > 0)
    .withColumn("new_york_avg_age_percent_rank", F.percent_rank().over(w6))
)

result = (
    summary
    .select(
        "city",
        "segment",
        "last4_min_age_tier",
        "corporate_rows",
        "new_york_rows",
        "new_york_avg_age",
        "avg_age_avg_3",
        "distinct_ids",
    )
    .orderBy("avg_age_avg_3")
)
