from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("cost").alias("value"))
    .intersect(
        df_transactions
        .filter(F.col("merchant").isNull())
        .select(F.col("amount").alias("value"))
    )
)
