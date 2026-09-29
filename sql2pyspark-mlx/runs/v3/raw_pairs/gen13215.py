from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.1)
    .select(
        "id",
        "discount",
        F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"),
    )
    .orderBy(F.col("id").desc())
)
