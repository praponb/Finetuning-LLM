from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isin([4, 21]))
    .groupBy("status", "account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy("account_type")
)
