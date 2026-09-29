from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") >= 0.16)
    .select(
        "id",
        "amount",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        F.round((F.col("discount") * 100), 2).alias("discount_adjusted"),
    )
    .orderBy(F.col("discount_adjusted").desc(), "id")
)
