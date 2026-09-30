from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("days_since_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy("days_since_opened_date", "id")
w4 = Window.partitionBy("account_type").orderBy("balance", "id")
w5 = Window.orderBy(F.col("days_since_opened_date").desc_nulls_first())

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2024-02-01").cast("date")) & F.col("customer_id").isin([10, 15, 22]))
    .withColumn("balance_band", F.when(F.col("balance") >= 84000, "High").when(F.col("balance") >= 18000, "Medium").otherwise("Low"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

scored = (
    valid
    .withColumn("running_days_since_opened_date", F.sum("days_since_opened_date").over(w))
    .withColumn("cumulative_balance", F.sum("balance").over(w2))
    .withColumn("days_since_opened_date_bucket", F.ntile(3).over(w3))
    .withColumn("prev_days_since_opened_date", F.lag("days_since_opened_date", 1).over(w4))
    .withColumn("days_since_opened_date_dense_rank", F.dense_rank().over(w5))
)

cleaned = (
    scored
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-05-10"))) & F.col("days_since_opened_date").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_band",
        "days_since_opened_date",
        "running_days_since_opened_date",
        "days_since_opened_date_dense_rank",
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "days_since_opened_date",
        "days_since_opened_date_dense_rank",
        "account_type_status_key",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("status").asc_nulls_first(), "days_since_opened_date_dense_rank")
)
