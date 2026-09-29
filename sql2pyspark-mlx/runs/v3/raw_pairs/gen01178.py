from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") < 26) & F.col("shipping_fee").between(17, 26))
    .select(
        "id",
        "status",
        "amount",
        F.round((F.col("amount") * 0.9), 2).alias("amount_scaled"),
    )
)
