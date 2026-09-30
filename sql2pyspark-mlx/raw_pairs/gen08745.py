from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy(F.col("order_date_month_rounded").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("status").orderBy(F.col("order_date_month_per_discount").desc(), "id")
w_hist = Window.partitionBy("store_id").orderBy("order_date_month_rounded", "id")
w_recent = Window.partitionBy("status").orderBy("unit_price", "id")

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.year("order_date").alias("order_date_year"),
        F.month("order_date").alias("order_date_month"),
    )
)

cleaned_orders = (
    cleaned
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-08-21"))) & F.col("channel").isNotNull())
    .withColumn("order_date_month_rounded", F.round("order_date_month", 2))
    .withColumn("order_date_month_per_discount", (F.col("order_date_month") / F.nullif("discount", F.lit(0))))
)

ranked = (
    cleaned_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "store_id",
        "status",
        "order_date_month_rounded",
        "order_date_month_per_discount",
    )
)

with_latest = (
    ranked
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

deduped = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .withColumn("prev_order_date_month_rounded", F.lag("order_date_month_rounded").over(w_hist))
    .withColumn("next_order_date_month_rounded", F.lead("order_date_month_rounded").over(w_hist))
    .withColumn("prev_order_date_month_per_discount", F.lag("order_date_month_per_discount").over(w_hist))
    .withColumn("next_order_date_month_per_discount", F.lead("order_date_month_per_discount").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("next_unit_price", F.lead("unit_price", 1).over(w_recent))
)

valid = (
    metrics
    .filter(F.col("store_id").isNotNull() & (F.col("prev_order_date_month_rounded") > 64))
    .withColumn("next_unit_price_adjusted", F.round((F.col("next_unit_price") * 0.9), 2))
    .withColumn("unit_price_whole", F.floor("unit_price"))
)

by_type = (
    valid
    .groupBy("store_id")
    .pivot("status", ["PENDING", "CANCELLED"])
    .agg(
        F.sum("prev_order_date_month_rounded").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "store_id",
        F.col("PENDING_value_sum").alias("pending_value_sum"),
        F.col("PENDING_cnt").alias("pending_cnt"),
        F.col("CANCELLED_value_sum").alias("cancelled_value_sum"),
        F.col("CANCELLED_cnt").alias("cancelled_cnt"),
    )
)

result = (
    by_type
    .select(
        "store_id",
        "pending_value_sum",
        "pending_cnt",
        "cancelled_value_sum",
        "cancelled_cnt",
    )
    .orderBy(F.col("cancelled_value_sum").desc_nulls_last())
)
