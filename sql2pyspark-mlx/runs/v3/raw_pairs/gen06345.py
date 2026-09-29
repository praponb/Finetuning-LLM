from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 13)
    .groupBy(
        F.when(F.col("status").isin(["CANCELLED", "RETURNED"]), "group_a").otherwise("other").alias("status_group"),
        "channel",
    )
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("shipping_fee_range") > 22)
    .orderBy(F.col("status_group").desc(), "channel")
)
