from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS", "LOAN"]))
    .groupBy(
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.countDistinct("balance").alias("unique_account_types"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("max_balance") > 49000)
    .drop("max_balance")
    .orderBy("status_group", "account_type_values")
)
