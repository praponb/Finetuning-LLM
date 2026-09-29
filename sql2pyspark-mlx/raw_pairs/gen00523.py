from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("order_id", "status").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("prev_amount").desc(), "id")
w6 = Window.partitionBy("status").orderBy("next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("event_seq_2").desc_nulls_last())

base = (
    df_payments
    .filter(F.col("order_id").isin([2, 3, 9, 32]))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id")
)

with_dims = (
    ranked
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.id", "e.order_id", F.col("p.amount"), F.col("p.status"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "status",
        F.lag("amount").over(w2).alias("prev_amount"),
        F.lead("amount").over(w2).alias("next_amount"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

scored = (
    with_windows
    .withColumn("first_known_prev_amount", F.first("prev_amount", ignorenulls=True).over(w4))
    .withColumn("event_seq_2", F.row_number().over(w5))
    .withColumn("first_known_prev_amount_2", F.first("prev_amount", ignorenulls=True).over(w6))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w7))
)

deduped = (
    scored
    .withColumn("row_num", F.rank().over(w8))
    .filter(F.col("row_num") == 1)
    .select("id", "prev_amount", "seq_no", "first_known_prev_amount_2")
)

result = (
    deduped
    .groupBy("seq_no")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("first_known_prev_amount_2", [0.25, 0.5, 0.75]).alias("first_known_prev_amount_2_quartiles"),
        F.count("*").alias("cnt"),
    )
)
