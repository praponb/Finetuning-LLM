from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

deduped = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "method", "amount")
)

result = (
    deduped
    .cube(
        F.when(F.col("amount") >= 1740, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_rows"),
    )
    .orderBy(F.col("amount_tier").desc())
)
