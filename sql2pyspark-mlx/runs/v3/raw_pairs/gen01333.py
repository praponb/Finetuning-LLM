from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.month("opened_date").isin([8, 10, 11]) & (F.col("opened_date") >= "2024-10-01"))
    .withColumn("abs_balance", F.abs("balance"))
)

accounts_stats = (
    base
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = accounts_stats.select("customer_id", "status_list")
