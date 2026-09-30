from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 75000)
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.avg("balance").alias("avg_balance"),
    )
    .filter(F.col("avg_balance") >= 33000)
    .drop("avg_balance")
    .orderBy(F.col("opened_quarter").desc())
    .limit(20)
)
