from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select(
        "id",
        "status",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount").alias("AMOUNT"),
        "shipping_fee",
    )
    .unpivot(
        ids=["id", "status"],
        values=["DISCOUNT_VAL", "AMOUNT", "shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)

result = (
    unpivoted
    .withColumn("val_tier", F.when(F.col("val") >= 63, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["val_tier", "status"], ["val_tier"], []], "val_tier", "status")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.countDistinct("status").alias("distinct_status_2"),
        F.count_if(F.col("metric_name") == "DISCOUNT_VAL").alias("discount_val_events"),
        F.count_if(F.col("metric_name") == "DISCOUNT_VAL").alias("discount_val_events_2"),
        F.grouping("val_tier").alias("grouping_id"),
    )
)
