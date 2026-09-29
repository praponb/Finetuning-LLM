from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 4500, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["session_id", "new_group_flag_tier", "amount_tier"], ["session_id", "new_group_flag_tier"], ["session_id"], []],
        "session_id",
        "new_group_flag_tier",
        "amount_tier",
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "7-Eleven", 1)), F.lit(0)), 2).alias("amount_per_7_eleven"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
    )
    .orderBy(F.col("amount_tier").desc_nulls_last())
)
