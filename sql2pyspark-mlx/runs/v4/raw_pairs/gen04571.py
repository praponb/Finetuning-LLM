from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1400, 2800))
    .select("revenue", F.lit("store_sale").alias("source"))
    .union(
        df_payments
        .filter(F.col("amount") > 630)
        .select("amount", F.lit("payment").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
