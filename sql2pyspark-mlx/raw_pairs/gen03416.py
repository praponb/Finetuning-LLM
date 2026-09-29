from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last())
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.to_date("txn_time"), "txn_type").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant")
w6 = Window.partitionBy(F.to_date("txn_time")).orderBy("island_id", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("txn_type").orderBy(F.col("new_group_flag").asc_nulls_first())
w8 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w9 = Window.partitionBy("txn_type").orderBy(F.col("island_id").desc())
w10 = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").desc(), "id")

latest = (
    df_transactions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w3))

metrics = (
    sessionized
    .select(
        "*",
        F.lead("amount", 1).over(w_ordered).alias("next_amount"),
        F.sum("island_id").over(w5).alias("merchant_sum_island_id"),
        F.avg("island_id").over(w6).alias("island_id_avg_2"),
        F.percent_rank().over(w_part).alias("new_group_flag_percent_rank"),
        F.lead("island_id", 1).over(w8).alias("next_island_id"),
    )
)

filtered = (
    metrics
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-10-20"))) & F.col("merchant").isNotNull())
    .withColumn("island_id_rounded", F.round("island_id", 1))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("N/A")))
)

result = (
    filtered
    .withColumn("merchant_count_new_group_flag", F.count("new_group_flag").over(w5))
    .withColumn("island_id_dense_rank", F.dense_rank().over(w9))
    .withColumn("row_seq", F.row_number().over(w10))
)
