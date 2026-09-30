from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("method").orderBy("session_id", "id")
w4 = Window.partitionBy("order_id").orderBy("island_id", "id")
w5 = Window.partitionBy("status", "order_id")
w6 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

scored = (
    islands
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.lag("session_id").over(w_grp).alias("prev_session_id"),
        F.lead("session_id").over(w_grp).alias("next_session_id"),
        F.lead("session_start_flag").over(w_grp).alias("next_session_start_flag"),
        F.lag("session_start_flag").over(w_grp).alias("prev_session_start_flag"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

flagged_2 = (
    scored
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged_2
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "next_session_start_flag",
        "seq_no",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

with_windows = (
    sessionized
    .filter(F.col("island_id").between(3, 8))
    .select(
        "id",
        "order_id",
        "next_session_start_flag",
        "seq_no",
        "island_id",
        F.lead("seq_no", 1).over(w4).alias("next_seq_no"),
        F.max("island_id").over(w5).alias("status_max_island_id"),
        F.max("seq_no").over(w6).alias("seq_no_max_30d"),
    )
)

result = (
    with_windows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "next_session_start_flag",
        "seq_no",
        "island_id",
        "next_seq_no",
        "status_max_island_id",
        "seq_no_max_30d",
        F.col("avg_order_item_quantity"),
    )
    .orderBy(F.col("avg_order_item_quantity").desc_nulls_first())
)
