from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy(F.date_trunc("YEAR", "opened_date")).orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w3 = Window.partitionBy("status")
w4 = Window.orderBy("balance", "id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

metrics = (
    df_accounts
    .select(
        "*",
        F.ntile(3).over(w_running).alias("balance_bucket"),
        F.cume_dist().over(w2).alias("balance_cume_dist"),
        F.sum("balance").over(w3).alias("status_sum_balance"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    metrics
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "balance_bucket",
        "balance_cume_dist",
        "status_sum_balance",
        "seq_no",
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("balance_cume_dist").desc_nulls_first())
)
