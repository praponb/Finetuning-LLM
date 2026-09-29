from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        F.col("amount").alias("AMOUNT"),
        F.col("shipping_fee").alias("SHIPPING_FEE"),
        F.col("discount").alias("DISCOUNT"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["AMOUNT", "SHIPPING_FEE", "DISCOUNT"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
