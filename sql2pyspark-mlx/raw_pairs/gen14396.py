from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "customer_id")
    .pivot("status", ["FROZEN", "OPEN", "CLOSED"])
    .agg(F.max("balance").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "account_type",
        "customer_id",
        F.col("FROZEN_value_sum").alias("frozen_value_sum"),
        F.col("FROZEN_item_count").alias("frozen_item_count"),
        F.col("OPEN_value_sum").alias("open_value_sum"),
        F.col("OPEN_item_count").alias("open_item_count"),
        F.col("CLOSED_value_sum").alias("closed_value_sum"),
        F.col("CLOSED_item_count").alias("closed_item_count"),
    )
)
