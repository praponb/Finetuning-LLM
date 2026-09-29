from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("seq_no").desc(), "id")
w3 = Window.partitionBy("customer_id", "status").orderBy("prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "status").orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-04-01")
    .select("*", F.add_months("start_date", 6).alias("start_date_plus_months"))
)

enriched = (
    cleaned
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "start_date_plus_months",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("seq_no"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "status",
        "prev_monthly_fee",
        "next_monthly_fee",
        "seq_no",
        F.row_number().over(w2).alias("event_seq"),
        F.sum("prev_monthly_fee").over(w3).alias("running_prev_monthly_fee"),
    )
)

valid = (
    with_windows
    .filter(F.col("status").isin(["CANCELLED", "PAUSED"]))
    .select(
        "id",
        "customer_id",
        "status",
        "prev_monthly_fee",
        "next_monthly_fee",
        "seq_no",
        "event_seq",
        "running_prev_monthly_fee",
        F.upper("status").alias("status_upper"),
    )
)

base = (
    valid
    .filter(F.col("prev_monthly_fee") >= 190)
    .withColumn("prev_monthly_fee_minus_running_prev_monthly_fee", (F.col("prev_monthly_fee") - F.col("running_prev_monthly_fee")))
)

enriched_subscriptions = (
    base
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.segment"), F.col("p.email"), F.col("p.city"))
)

metrics = (
    enriched_subscriptions
    .withColumn("last4_count_next_monthly_fee", F.count("next_monthly_fee").over(w4))
    .withColumn("last4_avg_next_monthly_fee", F.avg("next_monthly_fee").over(w4))
    .withColumn("last4_max_next_monthly_fee", F.max("next_monthly_fee").over(w4))
)

result = (
    metrics
    .groupBy("status")
    .pivot("city", ["New York", "Chiang Mai"])
    .agg(
        F.max("last4_count_next_monthly_fee").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "status",
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_cnt").alias("new_york_cnt"),
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_cnt").alias("chiang_mai_cnt"),
    )
)
