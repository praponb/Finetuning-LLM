from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date", F.lit("store_sale").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("region") == "Central")
        .select(F.col("opened_date").alias("sale_date"), F.lit("store").alias("source"))
    )
)
