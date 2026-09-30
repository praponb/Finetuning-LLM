from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

remaining_keys = (
    df_accounts
    .select("id", "status")
    .exceptAll(
        df_accounts
        .filter(F.col("customer_id").isin([3, 21, 37]))
        .select("id", "status")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "account_type", "status", F.col("min_order_amount"))
)
