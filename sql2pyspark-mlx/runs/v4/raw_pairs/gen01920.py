from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type", "status")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("week_start").orderBy(F.col("account_type_max_balance_p95").desc())

valid = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.upper("account_type").alias("account_type_upper"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.sum("balance").over(w_recent).alias("cumulative_balance"),
        F.max("balance").over(w2).alias("account_type_max_balance"),
        F.avg("balance").over(w3).alias("balance_avg_3"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("week", "opened_date").alias("week_start"),
        F.when(F.col("account_type_max_balance") >= 59925, "LARGE").otherwise("SMALL").alias("account_type_max_balance_tier"),
        F.when(F.col("cumulative_balance") >= 391581, "LARGE").otherwise("SMALL").alias("cumulative_balance_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.avg("balance_avg_3").alias("avg_balance_avg_3"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.percentile_approx("account_type_max_balance", 0.95).alias("account_type_max_balance_p95"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
    )
    .withColumn("account_type_max_balance_p95_percent_rank", F.percent_rank().over(w5))
    .orderBy(F.col("week_start").desc())
)
