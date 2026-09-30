from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")
w_running = Window.orderBy("days_since_order_date", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("month", "order_date")).orderBy("shipping_fee", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("discount").desc_nulls_first())

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id") == 40), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") >= F.col("s.order_date")), "left_anti")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "store_id",
        "order_date",
        "amount",
        "shipping_fee",
        "days_since_order_date",
        "latest_rank",
        F.avg("days_since_order_date").over(w_running).alias("days_since_order_date_avg_2_prior"),
        F.first("discount", ignorenulls=True).over(w3).alias("first_known_discount"),
        F.avg("amount").over(w4).alias("amount_avg_5_prior"),
        F.percent_rank().over(w_ordered).alias("discount_percent_rank"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.days_since_order_date_avg_2_prior")).alias("avg_days_since_order_date_avg_2_prior"),
    )
    .orderBy(F.col("rows_total").asc_nulls_last())
)
