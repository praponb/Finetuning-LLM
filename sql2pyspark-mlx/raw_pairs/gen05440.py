from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("status", "method").orderBy("amount", "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("amount_sum_30d").desc(), "id")
w6 = Window.partitionBy("order_id", "status").orderBy(F.col("running_amount").desc_nulls_last(), "id")
w7 = Window.partitionBy("order_id").orderBy("running_amount", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

with_windows = (
    df_payments
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.sum("amount").over(w3).alias("amount_sum_30d"),
        F.lag("amount", 2).over(w4).alias("prev_amount"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.store_id") == 30)
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.status",
        "s.running_amount",
        "s.prev_amount",
        F.col("b.order_date"),
        F.col("b.status").alias("order_status"),
    )
)

deduped_payments = (
    joined
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped_payments
    .withColumn("row_num_2", F.row_number().over(w7))
    .filter(F.col("row_num_2") == 1)
)

result = (
    ranked
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "prev_amount",
        "order_status",
        "row_num_2",
        F.when(F.col("order_status").rlike("^(RETURNED|PENDING)$"), "CORE").otherwise("EXTENDED").alias("order_status_zone"),
        F.col("max_shipment_cost"),
    )
)
