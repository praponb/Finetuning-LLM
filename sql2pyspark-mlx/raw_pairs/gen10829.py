from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select("id", "channel", F.col("amount").alias("AMOUNT"), "shipping_fee")
    .unpivot(
        ids=["id", "channel"],
        values=["AMOUNT", "shipping_fee"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    metrics_long
    .withColumn("val_tier", F.when(F.col("val") >= 69, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["metric", "val_tier"], ["metric"]], "metric", "val_tier")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count_if(F.col("metric") == "shipping_fee").alias("shipping_fee_events"),
        F.sort_array(F.collect_set("metric")).alias("metric_list"),
        F.count(F.when(F.col("metric") == "shipping_fee", 1)).alias("shipping_fee_rows"),
        F.sum(F.when(F.col("metric") == "shipping_fee", F.col("val")).otherwise(0)).alias("shipping_fee_val"),
        F.grouping("metric").alias("grouping_level"),
    )
)
