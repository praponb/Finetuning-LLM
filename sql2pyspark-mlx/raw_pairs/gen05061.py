from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id")
w_part = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

scored = (
    df_subscriptions
    .select(
        "*",
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("event_seq"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.row_number().over(w2).alias("seq_no"),
        F.lead("next_monthly_fee", 2).over(w).alias("next_next_monthly_fee"),
        F.sum("monthly_fee").over(w_part).alias("cumulative_monthly_fee"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.avg("seq_no").over(w4).alias("last7_avg_seq_no"),
        F.min("seq_no").over(w4).alias("last7_min_seq_no"),
        F.max("seq_no").over(w4).alias("last7_max_seq_no"),
        F.count("seq_no").over(w4).alias("last7_count_seq_no"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("status") == "EXPIRED")
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_subscriptions = (
    reconciled
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.customer_id",
        "s.plan",
        "s.next_next_monthly_fee",
        "s.cumulative_monthly_fee",
        "s.last7_avg_seq_no",
        "s.last7_max_seq_no",
        "s.last7_count_seq_no",
        F.col("d.name"),
        F.col("d.signup_date"),
    )
)

with_dims = (
    enriched_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.segment"))
)

result = (
    with_dims
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "plan",
        "last7_avg_seq_no",
        "last7_max_seq_no",
        "last7_count_seq_no",
        "segment",
        F.when(F.col("plan").rlike("^(enterprise|basic)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(standard|premium)$"), "GROUP_A").otherwise("CORE").alias("plan_zone"),
        F.col("max_order_amount"),
    )
)
