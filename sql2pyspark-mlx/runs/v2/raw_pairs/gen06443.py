from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "cash")
    .select("id", "amount", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
    .orderBy(F.col("amount").desc())
)
