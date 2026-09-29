from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("amount_stddev").desc())

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1310, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.when(F.col("discount") >= 0.44, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.count("*").alias("row_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .filter(F.col("num_channels") >= 1)
    .withColumn("amount_stddev_dense_rank", F.dense_rank().over(w))
)
