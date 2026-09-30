from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "OPEN")
    .groupBy("account_type")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
    )
)
