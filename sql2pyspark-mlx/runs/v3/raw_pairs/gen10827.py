from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "SHIPPED", "RETURNED"]) & (F.col("amount") < 960) & F.col("order_date").between("2025-03-20", "2025-10-01"))
    .select(
        "customer_id",
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
    )
)
