from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-02-01", "2025-07-15") & F.col("shipping_fee").isNotNull())
    .select(
        "channel",
        "shipping_fee",
        "status",
        F.round((F.col("amount") * 0.9), 2).alias("amount_adjusted"),
    )
    .orderBy(F.col("status").desc())
)
