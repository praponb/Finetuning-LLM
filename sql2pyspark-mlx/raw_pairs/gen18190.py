from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.col("amount").alias("value"), F.lit("payment").alias("record_type"))
    .unionByName(
        df_products
        .filter(F.col("stock_qty") < 262)
        .select(F.col("price").alias("value"), F.lit("product").alias("record_type"))
    )
)
