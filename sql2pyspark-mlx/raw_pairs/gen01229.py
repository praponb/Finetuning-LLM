from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Acme", "Umbrella", "Initech"])
    .agg(F.sum("stock_qty").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "category",
        F.col("Acme_total").alias("acme_total"),
        F.col("Acme_cnt").alias("acme_cnt"),
        F.col("Umbrella_total").alias("umbrella_total"),
        F.col("Umbrella_cnt").alias("umbrella_cnt"),
        F.col("Initech_total").alias("initech_total"),
        F.col("Initech_cnt").alias("initech_cnt"),
    )
)
