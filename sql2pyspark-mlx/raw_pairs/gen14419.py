from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("paid_at")).orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("paid_at", "id")
w5 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy(F.col("island_id").desc())
w7 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w8 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last(), "id")

enriched = (
    df_payments
    .select(
        "*",
        F.first("method", ignorenulls=True).over(w).alias("first_known_method"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.sum("amount").over(w3).alias("running_amount"),
    )
)

flagged = (
    enriched
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("session_start_flag").over(w5).alias("island_id"),
    )
)

latest = (
    islands
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

flagged_2 = (
    latest
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w7) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    flagged_2
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "island_id",
        "rank_in_group",
        F.sum("is_new_session").over(w8).alias("session_no"),
    )
)

latest_payments = (
    sessions_payments
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .cube(F.col("d.channel").alias("channel"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.amount")).alias("min_amount"),
        F.round(F.sum(F.col("src.session_no")), 2).alias("total_session_no"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("total_session_no").desc())
)
