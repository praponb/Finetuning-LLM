from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & F.col("customer_id").isNotNull())
    .withColumn("balance_tier", F.when(F.col("balance") >= 82000, "High").when(F.col("balance") >= 6000, "Medium").otherwise("Low"))
    .withColumn("years_since_opened_date", (F.datediff(F.current_date(), "opened_date") / 365.25))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("years_since_opened_date") >= 61, "HIGH").otherwise("LOW").alias("years_since_opened_date_tier"),
    )
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy(F.col("balance_stddev").desc_nulls_last())
)
