from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("method", "status")

enriched = (
    df_payments
    .withColumn("amount_avg_2_prior", F.avg("amount").over(w))
    .withColumn("amount_rank", F.rank().over(w2))
    .withColumn("method_stddev_samp_amount", F.stddev_samp("amount").over(w3))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_avg_2_prior",
        "method_stddev_samp_amount",
    )
    .orderBy(F.col("method").desc(), F.col("method_stddev_samp_amount").desc())
)
