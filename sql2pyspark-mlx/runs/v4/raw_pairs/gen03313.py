from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-10-07 03:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.length("method").alias("method_length"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .withColumn("method_length_tier", F.when(F.col("method_length") >= 86, "LARGE").otherwise("SMALL"))
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["method_length_tier", "burst_id_tier"], ["method_length_tier"], []],
        "method_length_tier",
        "burst_id_tier",
    )
    .agg(
        F.sum(F.when(F.col("method") == "card", F.col("burst_id")).otherwise(0)).alias("card_burst_id"),
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
    )
)
