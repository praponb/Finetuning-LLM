from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-04-28").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("status_lower", F.lower("status"))
    .withColumn("status_filled", F.coalesce("status", F.lit("NONE")))
)

result = (
    base
    .groupBy("customer_id")
    .pivot("account_type", ["LOAN", "CHECKING"])
    .agg(F.max("balance").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
