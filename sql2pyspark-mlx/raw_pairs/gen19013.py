from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select(
        "id",
        "channel",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount").alias("AMOUNT_VAL"),
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
    )
    .unpivot(
        ids=["id", "channel"],
        values=["DISCOUNT_VAL", "AMOUNT_VAL", "SHIPPING_FEE_VAL"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    metrics_long
    .cube(
        "metric",
        F.when(F.col("val") >= 78, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
        "channel",
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").desc_nulls_last())
)
