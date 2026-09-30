from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "status").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("discount", "id")
w5 = Window.partitionBy(F.date_trunc("week", "order_date")).orderBy("running_sum_discount", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("running_max_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("discount", "id")
w8 = Window.partitionBy("channel", "status").orderBy("shipping_fee", "id")

enriched = (
    df_orders
    .filter(F.col("channel") != "web")
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.max("discount").over(w).alias("running_max_discount"),
        F.count("discount").over(w).alias("running_count_discount"),
        F.sum("discount").over(w).alias("running_sum_discount"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

cleaned = (
    enriched
    .filter((F.col("order_date") > F.to_date(F.lit("2023-02-20"))) & F.col("channel").isNotNull())
    .select(
        "*",
        (F.col("running_sum_discount") - F.col("running_max_discount")).alias("running_sum_discount_minus_running_max_discount"),
        F.col("running_count_discount").cast("string").alias("running_count_discount_str"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

metrics = (
    cleaned
    .withColumn("last_known_running_sum_discount", F.last("running_sum_discount", ignorenulls=True).over(w3))
    .withColumn("next_amount", F.lead("amount", 2).over(w4))
    .withColumn("amount_avg_3", F.avg("amount").over(w5))
    .withColumn("running_running_count_discount", F.sum("running_count_discount").over(w6))
    .withColumn("prev_discount", F.lag("discount", 1, 0).over(w7))
)

enriched_orders = (
    metrics
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "discount",
        "running_max_discount",
        "running_sum_discount",
        "row_seq",
        "running_count_discount_str",
        "channel_filled",
        "last_known_running_sum_discount",
        "next_amount",
        F.lag("prev_discount").over(w8).alias("prev_prev_discount"),
        F.lead("prev_discount").over(w8).alias("next_prev_discount"),
        F.lag("discount").over(w8).alias("prev_discount_2"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

remaining_keys = (
    enriched_orders
    .select("status")
    .exceptAll(
        df_orders
        .filter(F.col("amount").isNull())
        .select("status")
    )
)

kept_rows = (
    enriched_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "status",
        "running_max_discount",
        "discount",
        F.col("amount").alias("AMOUNT_VAL"),
    )
    .unpivot(
        ids=["status"],
        values=["running_max_discount", "discount", "AMOUNT_VAL"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
