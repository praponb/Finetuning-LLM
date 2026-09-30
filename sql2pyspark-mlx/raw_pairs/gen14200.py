from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "channel",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount").alias("AMOUNT"),
        "shipping_fee",
    )
    .unpivot(
        ids=["id", "channel"],
        values=["DISCOUNT_VAL", "AMOUNT", "shipping_fee"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
