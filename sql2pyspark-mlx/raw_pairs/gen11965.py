from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        F.col("discount").alias("DISCOUNT"),
        F.col("amount").alias("AMOUNT"),
        "shipping_fee",
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["DISCOUNT", "AMOUNT", "shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
