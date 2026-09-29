from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("balance", "id")
w_hist = Window.orderBy(F.col("balance").desc(), "id")

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "OPEN"))
    .select(
        "*",
        F.trim("account_type").alias("clean_account_type"),
        F.lower("account_type").alias("account_type_lower"),
    )
)

cleaned = (
    valid
    .filter(F.col("opened_date").between("2024-12-12", "2025-07-01"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "clean_account_type",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.trim("account_type").alias("clean_account_type_2"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

metrics = (
    cleaned
    .withColumn("balance_bucket", F.ntile(4).over(w))
    .withColumn("balance_bucket_2", F.ntile(10).over(w_hist))
)

result = (
    metrics
    .groupBy("account_type", "customer_id")
    .pivot("status", ["CLOSED", "OPEN"])
    .agg(F.sum("balance"))
)
