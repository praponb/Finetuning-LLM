from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select(
        "id",
        "customer_id",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount").alias("AMOUNT_VAL"),
        "shipping_fee",
    )
    .unpivot(
        ids=["id", "customer_id"],
        values=["DISCOUNT_VAL", "AMOUNT_VAL", "shipping_fee"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("reading") >= 83, "LARGE").otherwise("SMALL").alias("reading_tier"),
        "metric",
    )
    .agg(
        F.sort_array(F.collect_set("metric")).alias("metric_list"),
        F.sum(F.when(F.col("metric") == "AMOUNT_VAL", F.col("reading")).otherwise(0)).alias("amount_val_reading"),
    )
    .orderBy(F.col("amount_val_reading").desc())
)
