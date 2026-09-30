from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("amount") < 570)
        .select("status", F.lit("payment").alias("source"))
    )
)
