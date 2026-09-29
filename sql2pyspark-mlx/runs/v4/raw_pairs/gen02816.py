from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("amount")
w_ordered_2 = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-10-14").cast("date"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

with_windows = (
    cleaned
    .withColumn("amount_rank", F.rank().over(w_ordered))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w_ordered_2))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy("txn_type")
    .agg(
        F.count_if(F.col("merchant") == "Grab").alias("grab_events"),
        F.min("last_known_amount").alias("min_last_known_amount"),
        F.max("last_known_amount").alias("max_last_known_amount"),
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount_rank")).otherwise(0)).alias("grab_amount_rank"),
        F.avg("amount").alias("avg_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .orderBy(F.col("txn_type").desc_nulls_first())
)
