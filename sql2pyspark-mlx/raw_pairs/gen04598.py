from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_ordered = Window.partitionBy(F.to_date("txn_time"), "txn_type").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

flagged = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w3))

enriched = (
    sessionized
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.status"), F.col("p.opened_date"), F.col("p.balance"))
)

scored = (
    enriched
    .select(
        "id",
        "txn_time",
        "merchant",
        "session_start_flag",
        "burst_id",
        "status",
        "opened_date",
        "balance",
        F.avg("burst_id").over(w4).alias("burst_id_avg_30d"),
        F.sum("amount").over(w_ordered).alias("cumulative_amount"),
    )
)

daily = (
    scored
    .withColumn("week_start", F.date_trunc("week", "opened_date"))
    .groupingSets([["burst_id", "week_start"], ["burst_id"]], "burst_id", "week_start")
    .agg(
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
        F.max("txn_time").alias("last_seen"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    daily
    .select("last_seen", "span_minutes", "7_eleven_count", "gid")
    .orderBy(F.col("span_minutes").desc())
)
