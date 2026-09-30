from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance_int", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("status").orderBy("balance_int", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("status").orderBy("is_savings", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("status").orderBy(F.col("balance_int").desc(), "id")
w5 = Window.partitionBy("status").orderBy("balance_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy(F.col("balance_int").desc())
w7 = Window.partitionBy("status").orderBy("event_seq", "id")
w8 = Window.partitionBy("status").orderBy("is_savings_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status")
w10 = Window.partitionBy("status").orderBy("balance_int_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED"]))
    .select(
        "id",
        "status",
        F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0).alias("is_savings"),
        F.col("balance").cast("int").alias("balance_int"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

metrics = (
    cleaned
    .filter(F.col("is_savings") < 80)
    .select(
        "*",
        F.avg("is_savings").over(w).alias("is_savings_avg_5_prior"),
        F.avg("balance_int").over(w2).alias("balance_int_avg_3_prior"),
        F.avg("is_savings").over(w3).alias("is_savings_avg_3_prior"),
        F.row_number().over(w4).alias("event_seq"),
        F.sum("balance_int").over(w5).alias("running_balance_int"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w6).alias("balance_int_percent_rank"),
        F.ntile(4).over(w7).alias("event_seq_bucket"),
        F.sum("event_seq").over(w8).alias("cumulative_event_seq"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.min("is_savings").over(w9).alias("status_min_is_savings"),
        F.first("running_balance_int", ignorenulls=True).over(w10).alias("first_known_running_balance_int"),
    )
)

result = (
    scored
    .select(
        "id",
        "status",
        "is_savings",
        "balance_int",
        "balance_int_avg_3_prior",
        "is_savings_avg_3_prior",
        "running_balance_int",
        "balance_int_percent_rank",
        "event_seq_bucket",
        F.when(F.col("status").rlike("^(FROZEN|OPEN)$"), "EXTENDED").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc())
)
