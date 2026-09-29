from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([1, 6, 11]))
    .select(
        "id",
        "status",
        "amount",
        "customer_id",
        F.when(F.col("status").isin(["RETURNED", "PENDING"]), "Priority").otherwise("Other").alias("status_group"),
    )
)
