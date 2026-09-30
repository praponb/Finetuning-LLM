from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.37) & (F.col("status") == "CANCELLED"))
    .select(
        "channel",
        F.when(F.col("channel") == "web", 1).otherwise(0).alias("is_web"),
    )
)
