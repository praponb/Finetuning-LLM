from pyspark.sql import functions as F

long_format = (
    df_orders
    .select(
        "id",
        "store_id",
        F.col("discount").alias("DISCOUNT"),
        F.col("amount").alias("AMOUNT_VAL"),
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["DISCOUNT", "AMOUNT_VAL", "SHIPPING_FEE_VAL"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)

result = (
    long_format
    .groupBy(
        F.when(F.col("reading") >= 87, "HIGH").otherwise("LOW").alias("reading_tier"),
    )
    .agg(
        F.count(F.when(F.col("metric") == "DISCOUNT", 1)).alias("discount_rows"),
        F.round(F.sum("reading") / F.nullif(F.count(F.when(F.col("metric") == "AMOUNT_VAL", 1)), F.lit(0)), 2).alias("reading_per_amount_val"),
        F.count(F.when(F.col("metric") == "SHIPPING_FEE_VAL", 1)).alias("shipping_fee_val_rows"),
    )
)
