from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy(F.year("order_date"), "customer_id").orderBy(F.col("shipping_fee").desc())
w3 = Window.partitionBy("channel", "status").orderBy("discount", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

with_windows = (
    df_orders
    .filter(F.col("order_date").between("2023-04-24", "2023-10-01"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        F.corr("shipping_fee", "amount").over(w).alias("corr_shipping_fee_amount"),
        F.dense_rank().over(w2).alias("shipping_fee_dense_rank"),
        F.ntile(10).over(w3).alias("discount_bucket"),
    )
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "status",
        "corr_shipping_fee_amount",
        "shipping_fee_dense_rank",
        "discount_bucket",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("min_account_balance").desc())
)
