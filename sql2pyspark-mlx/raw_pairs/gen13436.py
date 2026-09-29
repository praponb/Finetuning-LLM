from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id")
w3 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        F.stddev_samp("amount").over(w).alias("txn_type_stddev_samp_amount"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("amount").over(w4).alias("cumulative_amount"),
    )
)

result = (
    scored
    .groupingSets(
        [["merchant", "account_id"], ["merchant"], []],
        "merchant",
        "account_id",
    )
    .agg(
        F.min("cumulative_amount").alias("min_cumulative_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.grouping("merchant").alias("grouping_id"),
    )
    .orderBy(F.col("min_cumulative_amount").asc_nulls_last())
)
