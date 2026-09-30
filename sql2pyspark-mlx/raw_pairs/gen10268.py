from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("burst_id", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy(F.to_date("paid_at"), "method").orderBy(F.col("row_seq").desc())
w6 = Window.partitionBy("order_id").orderBy(F.col("is_cash").asc_nulls_first())
w7 = Window.partitionBy("order_id").orderBy("is_new_session", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("order_id", "status")

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("method") == "cash", 1).otherwise(0).alias("is_cash"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

metrics = (
    islands
    .filter(F.col("order_id") == 6)
    .select(
        "*",
        F.lead("amount", 2, 0).over(w3).alias("next_amount"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "is_new_session",
        F.dense_rank().over(w5).alias("row_seq_dense_rank"),
        F.cume_dist().over(w6).alias("is_cash_cume_dist"),
        F.avg("is_new_session").over(w7).alias("is_new_session_avg_2_prior"),
        F.count("next_amount").over(w8).alias("order_id_count_next_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "is_new_session",
        "is_cash_cume_dist",
    )
)
