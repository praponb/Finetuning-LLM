from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount_int").desc_nulls_first(), "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc_nulls_last(), "id")
w_grp = Window.partitionBy("order_id").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("order_amount").desc(), "id")
w_running = Window.partitionBy("order_id", "order_status").orderBy(F.col("amount_int").desc(), "id")
w_grp_2 = Window.partitionBy("order_id").orderBy("order_amount", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

base = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-11-09").cast("date"))
    .withColumn("method_status", F.concat("method", F.lit(" - "), "status"))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base_payments = (
    latest
    .filter(F.col("method").isNotNull())
    .withColumn("amount_int_filled", F.coalesce("amount_int", F.lit(0)))
    .withColumn("status_prefix", F.substring("status", 1, 2))
    .withColumn("resolved_method", F.coalesce("method", F.lit("OTHER")))
)

latest_payments = (
    base_payments
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "amount_int",
        "amount_int_filled",
        "resolved_method",
    )
)

joined = (
    latest_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .filter(F.year(F.col("o.order_date")) >= 2024)
    .select(
        "src.id",
        "src.order_id",
        "src.amount",
        "src.status",
        "src.amount_int",
        "src.amount_int_filled",
        "src.resolved_method",
        F.col("o.amount").alias("order_amount"),
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.last("order_status", ignorenulls=True).over(w_grp).alias("last_known_order_status"),
        F.ntile(3).over(w4).alias("order_amount_bucket"),
        F.row_number().over(w_running).alias("seq_no"),
        F.lead("amount_int_filled", 2).over(w_grp_2).alias("next_amount_int_filled"),
    )
)

filtered = (
    with_windows
    .filter(F.col("order_id").isNotNull() & F.col("amount_int").between(70, 71))
    .select(
        "id",
        "order_id",
        "status",
        "amount_int",
        "amount_int_filled",
        "resolved_method",
        "order_amount",
        "discount",
        "order_status",
        "last_known_order_status",
        "order_amount_bucket",
        "seq_no",
        "next_amount_int_filled",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

result = (
    filtered
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "status",
        "amount_int",
        "amount_int_filled",
        "resolved_method",
        "order_amount",
        "order_status",
        "last_known_order_status",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("amount_int").desc())
)
