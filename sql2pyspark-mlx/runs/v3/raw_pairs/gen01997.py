from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5]))
    .select("sale_date", F.lit("store_sale").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.year("end_date") >= 2023)
        .select(F.col("end_date").alias("sale_date"), F.lit("subscription").alias("source"))
    )
    .orderBy("sale_date")
)
