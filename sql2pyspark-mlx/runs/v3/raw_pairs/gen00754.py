from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .select(F.col("amount").alias("value"), F.lit("payment").alias("source"))
    .unionByName(
        df_store_sales
        .filter(F.month("sale_date").isin([1, 10, 12]))
        .select(F.col("revenue").alias("value"), F.lit("store_sale").alias("source"))
    )
)
