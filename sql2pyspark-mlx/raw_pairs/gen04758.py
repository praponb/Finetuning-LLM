from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "FROZEN"]) & ~F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.countDistinct("balance").alias("unique_account_types"),
    )
    .orderBy("savings_balance")
)
