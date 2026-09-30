from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("amount_filled", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("order_id", "status").orderBy("shipping_fee", "id")
w7 = Window.partitionBy("order_id", "status").orderBy("session_id", "id")

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-22 17:00:00").cast("timestamp")) & (F.col("amount") <= 880))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

gaps = (
    islands
    .withColumn("session_start_flag_2", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id_2", F.sum("session_start_flag_2").over(w4))

with_dims = (
    sessionized
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select("src.*", F.col("d.shipping_fee"), F.col("d.status").alias("order_status"))
)

with_windows = (
    with_dims
    .filter(F.col("status") != "REFUNDED")
    .select(
        "*",
        F.avg("session_id").over(w5).alias("session_id_avg_6"),
        F.lead("session_id_2", 2).over(w6).alias("next_session_id_2"),
        F.lag("amount_filled", 1).over(w7).alias("prev_amount_filled"),
    )
)

stats = (
    with_windows
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.count(F.when(F.col("order_status") == "RETURNED", 1)).alias("returned_rows"),
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("session_id_avg_6"))).alias("success_total_session_id_avg_6"),
    )
    .filter(F.col("min_amount") < 1000000)
)

result = stats.select("year_start", "success_total_session_id_avg_6")
