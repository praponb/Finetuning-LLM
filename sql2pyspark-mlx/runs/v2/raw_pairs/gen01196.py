from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
    .filter(F.col("checking_balance") < 57)
    .drop("checking_balance")
    .orderBy(F.col("customer_id").desc(), F.col("last_opened_date").desc())
)
