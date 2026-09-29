from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~((F.col("amount") < 1430) | (F.col("amount") > 440)))
    .select(
        "id",
        "discount",
        "amount",
        "status",
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
    )
)
