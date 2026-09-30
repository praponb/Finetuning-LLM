from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .orderBy(F.col("balance_range").desc(), "opened_quarter")
    .limit(50)
)
