from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessions_transactions
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count_if(F.col("merchant") == "Amazon").alias("amazon_events"),
        F.countDistinct("merchant").alias("distinct_merchants"),
    )
)
