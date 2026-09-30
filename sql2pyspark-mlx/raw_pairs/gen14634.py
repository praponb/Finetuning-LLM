from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("balance", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w3 = Window.partitionBy("account_type").orderBy("balance", "id")
w4 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("merchant").orderBy(F.col("event_seq").asc_nulls_first(), "id")

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-16").cast("date")) & F.col("status").isNotNull())
    .withColumn("opened_date_minus_57d", F.date_sub("opened_date", 57))
    .withColumn("status_group", F.when(F.col("status").isin(["FROZEN", "OPEN"]), "Core").otherwise("Other"))
)

metrics = (
    valid
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_6_prior"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(4).over(w3).alias("balance_bucket"),
        F.lead("balance", 1).over(w4).alias("next_balance"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.col("c.is_active"))
    .select(
        "src.id",
        "src.customer_id",
        "src.opened_date_minus_57d",
        "src.status_group",
        "src.balance_avg_6_prior",
        "src.event_seq",
        "src.next_balance",
        F.col("c.segment"),
    )
)

with_latest = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"))
)

latest = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.segment").alias("customer_segment"),
        F.col("a.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.next_balance")).alias("min_next_balance"),
    )
)
