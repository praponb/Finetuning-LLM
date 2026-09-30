from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 25]))
    .groupBy("account_type", "status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("opened_date").alias("last_opened_date"),
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
    )
)
