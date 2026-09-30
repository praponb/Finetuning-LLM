from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("status", "method")
w4 = Window.orderBy("amount", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("status", "method").orderBy("amount_share_2", "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-05-16").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "status",
        F.concat("method", F.lit("/"), "status").alias("method_status"),
    )
)

metrics = (
    filtered
    .filter(F.col("amount").between(510, 1540))
    .select(
        "id",
        "method",
        "status",
        "method_status",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.row_number().over(w2).alias("row_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share_2"),
        F.avg("amount").over(w4).alias("amount_avg_6_prior"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "method",
        "status",
        "amount_share",
        "row_seq",
        "amount_share_2",
        "amount_avg_6_prior",
    )
)

cleaned = (
    deduped
    .filter(F.col("amount_share_2") >= 0.66)
    .select(
        "id",
        "method",
        "status",
        "row_seq",
        (F.col("amount_share") * F.col("amount_share_2")).alias("amount_share_times_amount_share_2"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

filtered_payments = (
    cleaned
    .filter(F.col("method") != "cash")
    .select(
        "id",
        "method",
        "status",
        "row_seq",
        F.col("row_seq").cast("string").alias("row_seq_str"),
    )
)

base = (
    filtered_payments
    .filter(F.col("row_seq") < 6)
    .select(
        "id",
        "method",
        "row_seq",
        "row_seq_str",
        F.concat("method", F.lit(" - "), "status").alias("method_status"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.max("row_seq").alias("max_row_seq"),
        F.sum(F.when(F.col("method") == "cash", F.col("row_seq")).otherwise(0)).alias("cash_row_seq"),
        F.count_if(F.col("method") == "promptpay").alias("promptpay_events"),
        F.avg("row_seq").alias("avg_row_seq"),
    )
    .orderBy(F.col("cash_row_seq").desc())
)
