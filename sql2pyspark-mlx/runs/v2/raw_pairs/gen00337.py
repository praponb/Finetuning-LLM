from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "status",
        "customer_id",
        "shipping_fee",
        F.add_months("order_date", 12).alias("order_date_plus_months"),
        F.when(F.col("status").isin(["COMPLETED", "RETURNED"]), "Priority").otherwise("Other").alias("status_group"),
    )
    .orderBy("customer_id")
)
