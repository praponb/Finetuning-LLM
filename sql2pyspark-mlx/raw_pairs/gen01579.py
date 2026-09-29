from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-16").cast("date"))
    .select(
        "*",
        F.length("status").alias("status_length"),
        (F.datediff(F.current_date(), "opened_date") / 30).alias("months_since_opened_date"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

result = (
    cleaned
    .groupBy("status")
    .pivot("account_type", ["CHECKING", "SAVINGS"])
    .agg(F.sum("status_length").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "status",
        F.col("CHECKING_value_sum").alias("checking_value_sum"),
        F.col("CHECKING_cnt").alias("checking_cnt"),
        F.col("SAVINGS_value_sum").alias("savings_value_sum"),
        F.col("SAVINGS_cnt").alias("savings_cnt"),
    )
)
