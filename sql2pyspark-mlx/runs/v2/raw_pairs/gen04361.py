from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("method").isin(["paypal", "card", "bank_transfer"])).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("store_id").desc(), "status", "id")
    .limit(1)
)
