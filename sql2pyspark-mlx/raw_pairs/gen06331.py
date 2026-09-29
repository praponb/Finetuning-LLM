from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.month("opened_date").isin([7, 8, 10]) & (F.col("status") == "OPEN"))
)

agg = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    agg
    .filter(F.col("unique_statuss") > 4)
    .select("account_type", "unique_statuss", "status_list")
    .orderBy("account_type")
    .limit(10)
)
