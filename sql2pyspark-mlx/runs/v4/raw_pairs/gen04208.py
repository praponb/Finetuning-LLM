from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("order_date_minus_41d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("order_date_minus_41d", "id")
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w5 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("order_date_minus_41d", "id")
w7 = Window.partitionBy("channel").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("cumulative_amount", "id")
w9 = Window.partitionBy("customer_id", "status").orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-04-06")))
    .select(
        "*",
        F.date_sub("order_date", 41).alias("order_date_minus_41d"),
        F.when(F.col("channel").isin(["web", "partner"]), "Core").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.sum("amount").over(w_ordered).alias("cumulative_amount"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("discount", ignorenulls=True).over(w3).alias("first_known_discount"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.country"), F.col("c.credit_limit"))
)

kept = (
    with_dims
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < "2024-03-01"), "left_semi")
)

with_windows = (
    kept
    .select(
        "*",
        F.sum("discount").over(w5).alias("running_discount"),
        F.lead("first_known_discount", 1, 0).over(w6).alias("next_first_known_discount"),
        F.first("discount", ignorenulls=True).over(w7).alias("first_known_discount_2"),
        F.lag("amount", 1).over(w8).alias("prev_amount"),
        F.sum("event_seq").over(w9).alias("running_event_seq"),
    )
)

totals = (
    with_windows
    .withColumn("activity_month", F.date_trunc("month", "order_date"))
    .cube("activity_month")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.sum(F.when(F.col("channel") == "partner", F.col("prev_amount"))).alias("partner_total_prev_amount"),
        F.grouping("activity_month").alias("gid"),
    )
)

result = (
    totals
    .select("activity_month", "returned_count", "partner_total_prev_amount")
    .orderBy("activity_month")
)
