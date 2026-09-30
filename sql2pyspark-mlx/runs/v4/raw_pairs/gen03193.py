from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-06-29").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.when(F.col("status").isin(["REFUNDED", "SUCCESS"]), "Core").otherwise("Other").alias("status_group"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("burst_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.max(F.when(F.col("method") == "promptpay", F.col("is_new_burst"))).alias("promptpay_max_is_new_burst"),
        F.min("paid_at").alias("started_at"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.max("paid_at").alias("ended_at"),
    )
)
