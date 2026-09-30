from pyspark.sql import functions as F

result = (
    df_products
    .select("price", F.lit("product").alias("record_type"))
    .union(
        df_payments
        .filter(F.col("method") != "bank_transfer")
        .select("amount", F.lit("payment").alias("record_type"))
    )
)
