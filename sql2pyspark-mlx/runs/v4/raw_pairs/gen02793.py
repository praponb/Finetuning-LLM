from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

latest_match = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipped_date").alias("shipment_shipped_date"),
        F.col("c.cost").alias("shipment_cost"),
    )
)

result = (
    latest_match
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "shipping_fee",
        "shipment_cost",
        F.when(F.col("status").rlike("^(SHIPPED|RETURNED)$"), "EXTENDED").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
)
