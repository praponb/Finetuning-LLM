from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNull() & (F.col("customer_id") == 30))
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
    )
    .filter(F.col("loan_balance") < 74)
    .orderBy("loan_balance")
)
