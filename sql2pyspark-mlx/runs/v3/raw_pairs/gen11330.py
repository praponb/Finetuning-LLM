from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "shipping_fee",
        "status",
        F.round((F.col("amount") * 1.1), 2).alias("amount_scaled"),
    )
)
