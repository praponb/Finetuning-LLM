from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") > 0.16) & (F.col("discount") > 0.29))
    .select(
        "id",
        "discount",
        "channel",
        F.when(F.col("discount") >= 0.34, "High").when(F.col("discount") >= 0.13, "Medium").otherwise("Low").alias("discount_band"),
        F.round("discount", 0).alias("discount_rounded"),
    )
    .orderBy(F.col("channel").desc(), F.col("id").desc())
    .limit(1)
)
