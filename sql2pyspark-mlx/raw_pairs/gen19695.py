from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-03-30").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.quarter("opened_date").alias("opened_date_quarter"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

result = (
    cleaned
    .groupBy("customer_id")
    .pivot("status", ["CLOSED", "FROZEN"])
    .agg(F.max("balance").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
