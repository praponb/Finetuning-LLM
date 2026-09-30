from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-07-03"))) & F.col("method").isNull())
    .withColumn("amount_int", F.col("amount").cast("int"))
)

scored = (
    valid
    .withColumn("first_known_amount_int", F.first("amount_int", ignorenulls=True).over(w))
    .withColumn("next_amount", F.lead("amount", 1).over(w2))
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w3))
    .withColumn("order_id_sum_amount_int", F.sum("amount_int").over(w4))
)

prev_events = scored.withColumn("prev_paid_at", F.lag("paid_at").over(w5))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_id", F.sum("is_new_session").over(w))

base = (
    sessions_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.to_date("prev_paid_at").alias("prev_paid_at_date"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

result = (
    base
    .groupBy(
        "session_id",
        F.date_trunc("quarter", "prev_paid_at_date").alias("quarter_start"),
        F.date_trunc("year", "prev_paid_at_date").alias("year_start"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.round(F.sum("order_id_sum_amount_int") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("order_id_sum_amount_int_per_card"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .filter(F.col("distinct_order_ids") >= 1)
    .orderBy(F.col("distinct_order_ids").desc())
)
