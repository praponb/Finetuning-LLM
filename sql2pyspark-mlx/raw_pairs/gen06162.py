from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-22").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .pivot("account_type", ["SAVINGS", "CHECKING"])
    .agg(
        F.sum("days_since_opened_date").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
