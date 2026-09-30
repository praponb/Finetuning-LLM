from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "discount",
        "customer_id",
        (F.col("amount") + F.col("discount")).alias("amount_plus_discount"),
    )
)
