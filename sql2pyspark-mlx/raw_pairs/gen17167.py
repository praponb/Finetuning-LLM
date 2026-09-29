from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "channel",
        F.col("amount").alias("AMOUNT"),
        F.col("shipping_fee").alias("SHIPPING_FEE"),
    )
    .unpivot(
        ids=["id", "channel"],
        values=["AMOUNT", "SHIPPING_FEE"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
