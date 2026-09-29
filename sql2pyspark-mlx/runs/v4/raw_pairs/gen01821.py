from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("cost").alias("value"))
    .subtract(
        df_payments
        .filter(F.col("order_id").isNull())
        .select(F.col("amount").alias("value"))
    )
)
