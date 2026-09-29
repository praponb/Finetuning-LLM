from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("method")
w2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("amount_avg_6").desc_nulls_last(), "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-09-18").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.substring("method", 1, 4).alias("method_prefix"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

enriched = (
    filtered
    .filter(F.col("method") == "cash")
    .withColumn("method_sum_amount", F.sum("amount").over(w_grp))
    .withColumn("amount_avg_6", F.avg("amount").over(w2))
)

flagged = (
    enriched
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w4))

ranked = (
    islands
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select("id", "method", "method_prefix", "burst_id")
)

result = ranked.select("id", "method", "method_prefix")
