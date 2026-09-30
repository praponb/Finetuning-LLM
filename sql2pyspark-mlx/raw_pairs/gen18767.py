from pyspark.sql import functions as F

long_format = (
    df_orders
    .select(
        "id",
        "status",
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        F.col("amount").alias("AMOUNT_VAL"),
        "discount",
    )
    .unpivot(
        ids=["id", "status"],
        values=["SHIPPING_FEE_VAL", "AMOUNT_VAL", "discount"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    long_format
    .groupBy(
        "status",
        F.when(F.col("val") >= 74, "LARGE").otherwise("SMALL").alias("val_tier"),
    )
    .agg(
        F.count(F.when(F.col("measure") == "SHIPPING_FEE_VAL", 1)).alias("shipping_fee_val_count"),
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.count_if(F.col("measure") == "SHIPPING_FEE_VAL").alias("shipping_fee_val_events"),
    )
    .filter(F.col("shipping_fee_val_events") > 1)
    .orderBy(F.col("shipping_fee_val_count").desc_nulls_first())
)
