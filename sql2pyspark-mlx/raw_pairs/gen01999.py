from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee")
w2 = Window.partitionBy("plan", "subscription_plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_subscriptions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "plan", "start_date", "end_date")
)

matched = (
    deduped
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    matched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.age"))
)

metrics = (
    enriched
    .filter(F.col("age") == 41)
    .select(
        "id",
        "customer_id",
        "end_date",
        "subscription_id",
        "subscription_plan",
        F.sum("age").over(w2).alias("running_sum_age"),
        F.count("age").over(w2).alias("running_count_age"),
        F.avg("age").over(w2).alias("running_avg_age"),
    )
)

result = (
    metrics
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "subscription_plan"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("s.subscription_plan").alias("subscription_plan"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("s.running_sum_age")).alias("avg_running_sum_age"),
        F.max(F.col("s.subscription_id")).alias("max_subscription_id"),
    )
    .orderBy(F.col("max_subscription_id").desc_nulls_first())
)
