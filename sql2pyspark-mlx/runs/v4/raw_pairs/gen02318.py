from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at_weekofyear").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("paid_at", "id")
w5 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status", "order_id").orderBy("paid_at_weekofyear", "id")
w7 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w8 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.partitionBy("order_id", "method")
w10 = Window.partitionBy("method").orderBy(F.col("paid_at_weekofyear").desc(), "id")

cleaned = (
    df_payments
    .filter(F.col("paid_at") < "2024-04-01")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.weekofyear("paid_at").alias("paid_at_weekofyear"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

scored = (
    cleaned
    .withColumn("amount_bucket", F.ntile(5).over(w))
    .withColumn("paid_at_weekofyear_dense_rank", F.dense_rank().over(w2))
    .withColumn("first_known_paid_at_weekofyear", F.first("paid_at_weekofyear", ignorenulls=True).over(w3))
)

flagged = (
    scored
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w5))

result = (
    sessions_payments
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("next_new_group_flag", F.lead("new_group_flag", 1).over(w7))
    .withColumn("first_known_paid_at_weekofyear_count_30d", F.count("first_known_paid_at_weekofyear").over(w8))
    .withColumn("order_id_max_burst_id", F.max("burst_id").over(w9))
    .withColumn("event_seq", F.row_number().over(w10))
)
