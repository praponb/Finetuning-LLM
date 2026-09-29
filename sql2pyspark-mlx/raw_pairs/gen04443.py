from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee", F.lit("order").alias("source"))
    .union(
        df_transactions
        .filter(F.col("merchant").isin(["Grab", "Amazon", "Starbucks"]))
        .select("amount", F.lit("transaction").alias("source"))
    )
)
