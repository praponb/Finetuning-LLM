from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("balance") >= 52000) & (F.col("status") == "CLOSED")) | F.month("opened_date").isin([1, 2, 5]))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("status_values").desc(), "latest_opened_date")
)
