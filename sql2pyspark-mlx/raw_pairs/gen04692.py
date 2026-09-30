from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_type_length_times_amount", "id")
w2 = Window.orderBy(F.col("amount").desc(), "id")
w3 = Window.orderBy("latest_rank", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("account_id").orderBy(F.col("txn_type_length_times_amount").desc(), "id")
w5 = Window.partitionBy("account_id")
w_running = Window.partitionBy("account_id").orderBy("latest_rank", "id")

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-08-29").cast("date"))
    .withColumn("txn_type_length", F.length("txn_type"))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

filtered = (
    base
    .filter((F.col("txn_time") >= F.lit("2023-07-18 13:00:00").cast("timestamp")) & (F.col("txn_type") == "DEBIT"))
    .select("*", F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"))
)

base_transactions = (
    filtered
    .filter((F.col("txn_time") > F.lit("2023-09-06 11:00:00").cast("timestamp")) & (~F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "txn_type_length",
        "txn_time_label",
        (F.col("txn_type_length") * F.col("amount")).alias("txn_type_length_times_amount"),
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

ranked = (
    base_transactions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "account_id",
        "is_flagged",
        "txn_type_length",
        "txn_type_length_times_amount",
        "txn_time_day",
        "latest_rank",
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.avg("amount").over(w3).alias("amount_avg_6_prior"),
        F.ntile(10).over(w4).alias("txn_type_length_times_amount_bucket"),
        F.max("amount").over(w5).alias("account_id_max_amount"),
    )
)

result = (
    scored
    .withColumn("next_amount_avg_6_prior", F.lead("amount_avg_6_prior").over(w_running))
    .withColumn("next_txn_type_length", F.lead("txn_type_length").over(w_running))
    .withColumn("prev_txn_type_length", F.lag("txn_type_length").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
)
