from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.min("balance").alias("min_balance"),
    )
    .orderBy(F.col("balance_range").desc())
)
