from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc_nulls_last(), "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

latest = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.col("sum_order_shipping_fee"),
    )
)
