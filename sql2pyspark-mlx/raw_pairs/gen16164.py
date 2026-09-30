from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "status").orderBy("shipping_fee", "id").rowsBetween(-5, -1)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

with_windows = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "web"]))
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.avg("amount").over(w2).alias("amount_avg_5_prior"),
    )
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "amount_avg_5_prior",
        F.col("max_account_balance"),
    )
    .orderBy(F.col("amount_avg_5_prior").desc_nulls_first(), F.col("status").desc())
)
