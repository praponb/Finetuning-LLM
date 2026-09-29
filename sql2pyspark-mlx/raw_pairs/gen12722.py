from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan").orderBy("start_date", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "start_date"), "plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "customer_id").orderBy("running_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("start_date").desc(), "id")
w6 = Window.partitionBy("status", "customer_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.lead("monthly_fee").over(w_recent).alias("next_monthly_fee"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
)

base = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .withColumn("event_seq_times_running_monthly_fee", (F.col("event_seq") * F.col("running_monthly_fee")))
)

enriched_subscriptions = (
    base
    .select(
        "id",
        "end_date",
        "status",
        "next_monthly_fee",
        "event_seq_times_running_monthly_fee",
        F.sum("event_seq_times_running_monthly_fee").over(w3).alias("cumulative_event_seq_times_running_monthly_fee"),
        F.sum("running_monthly_fee").over(w4).alias("cumulative_running_monthly_fee"),
        F.row_number().over(w5).alias("seq_no"),
        F.first("running_monthly_fee", ignorenulls=True).over(w6).alias("first_known_running_monthly_fee"),
    )
)

filtered = (
    enriched_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "ACTIVE", "CANCELLED"]))
    .select("*", F.substring("status", 1, 2).alias("status_prefix"))
)

result = (
    filtered
    .select(
        "id",
        "end_date",
        "status",
        "next_monthly_fee",
        "cumulative_running_monthly_fee",
        "seq_no",
        "first_known_running_monthly_fee",
        "status_prefix",
        F.when(F.col("status").rlike("^(EXPIRED|CANCELLED)$"), "GROUP_A").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("seq_no").desc(), "end_date")
)
