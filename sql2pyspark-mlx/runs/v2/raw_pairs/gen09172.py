from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([11, 21, 39]))
    .groupBy("status")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.min("balance").alias("min_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
