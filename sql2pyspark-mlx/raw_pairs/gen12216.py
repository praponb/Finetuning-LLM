from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy("running_sum_monthly_fee", "id")
w4 = Window.partitionBy("customer_id").orderBy("running_max_monthly_fee", "id")
w5 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").asc_nulls_last(), "id")
w6 = Window.partitionBy("status").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("end_date", "id")

enriched = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.email"), F.col("d.name"))
)

enriched_subscriptions = (
    enriched
    .withColumn("running_min_monthly_fee", F.min("monthly_fee").over(w_part))
    .withColumn("running_sum_monthly_fee", F.sum("monthly_fee").over(w_part))
    .withColumn("running_max_monthly_fee", F.max("monthly_fee").over(w_part))
)

scored = (
    enriched_subscriptions
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.lag("running_max_monthly_fee", 1).over(w3).alias("prev_running_max_monthly_fee"),
        F.lead("running_min_monthly_fee", 1).over(w4).alias("next_running_min_monthly_fee"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .filter(F.col("customer_id") == 37)
    .select(
        "id",
        "customer_id",
        "status",
        "running_min_monthly_fee",
        "event_seq",
        F.first("next_running_min_monthly_fee", ignorenulls=True).over(w6).alias("first_known_next_running_min_monthly_fee"),
        F.last("next_running_min_monthly_fee", ignorenulls=True).over(w7).alias("last_known_next_running_min_monthly_fee"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

enriched_subscriptions_2 = (
    metrics
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.status",
        "src.event_seq",
        "src.last_known_next_running_min_monthly_fee",
        F.col("b.credit_limit"),
    )
)

result = (
    enriched_subscriptions_2
    .groupBy(
        F.when(F.col("event_seq") >= 7, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("status") == "ACTIVE").alias("active_events"),
        F.sort_array(F.collect_set("status")).alias("status_list_2"),
    )
)
