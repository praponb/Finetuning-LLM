from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.year("start_date"), "customer_id").orderBy(F.col("start_date").desc(), "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

scored = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .select(
        "*",
        F.max("monthly_fee").over(w_recent).alias("last4_max_monthly_fee"),
        F.avg("monthly_fee").over(w_recent).alias("last4_avg_monthly_fee"),
        F.min("monthly_fee").over(w_recent).alias("last4_min_monthly_fee"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    scored
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "last4_max_monthly_fee",
        "last4_min_monthly_fee",
        "row_seq",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("monthly_fee").desc_nulls_first())
)
