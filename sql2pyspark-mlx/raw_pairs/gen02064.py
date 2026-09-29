from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan", "customer_id").orderBy(F.col("start_date").desc(), "id")
w3 = Window.partitionBy(F.month("start_date"), "customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w4 = Window.partitionBy(F.date_trunc("week", "start_date"), "status").orderBy("start_date", "id")
w5 = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee_percent_rank").asc_nulls_last(), "id")
w6 = Window.partitionBy("customer_id")
w7 = Window.partitionBy(F.date_trunc("MONTH", "end_date"), "customer_id").orderBy("last_known_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("event_seq", "id")
w9 = Window.partitionBy("customer_id").orderBy("last_known_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.row_number().over(w2).alias("event_seq"),
        F.percent_rank().over(w3).alias("monthly_fee_percent_rank"),
        F.lead("monthly_fee", 1, 0).over(w4).alias("next_monthly_fee"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "customer_id",
        "end_date",
        "last_known_monthly_fee",
        "event_seq",
        "monthly_fee_percent_rank",
        "next_monthly_fee",
    )
)

scored_subscriptions = (
    deduped
    .select(
        "id",
        "customer_id",
        "end_date",
        "last_known_monthly_fee",
        F.round((F.col("event_seq") / F.sum("event_seq").over(w6)), 4).alias("event_seq_share"),
        F.sum("last_known_monthly_fee").over(w7).alias("running_last_known_monthly_fee"),
        F.ntile(4).over(w8).alias("event_seq_bucket"),
        F.last("next_monthly_fee", ignorenulls=True).over(w9).alias("last_known_next_monthly_fee"),
    )
)

joined = (
    scored_subscriptions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.end_date",
        "s.last_known_monthly_fee",
        "s.last_known_next_monthly_fee",
        F.col("c.city"),
        F.col("c.country"),
    )
)

stats = (
    joined
    .groupBy(
        F.when(F.col("last_known_next_monthly_fee") >= 232, "HIGH").otherwise("LOW").alias("last_known_next_monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("last_known_monthly_fee", [0.25, 0.5, 0.75]).alias("last_known_monthly_fee_quartiles"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)

result = (
    stats
    .select(
        "last_known_next_monthly_fee_tier",
        "last_known_monthly_fee_quartiles",
        "sg_count",
        "num_countries",
    )
    .orderBy(F.col("num_countries").desc_nulls_first())
)
