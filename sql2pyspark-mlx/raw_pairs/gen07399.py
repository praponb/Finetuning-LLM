from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .select(F.col("cost").alias("value"))
    .unionByName(
        df_payments
        .filter(F.col("method") == "card")
        .select(F.col("amount").alias("value"))
    )
    .orderBy(F.col("value").desc())
)
