from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.col("region").alias("label"))
    .subtract(
        df_payments
        .filter(F.col("method") != "paypal")
        .select(F.col("status").alias("label"))
    )
    .orderBy("label")
)
