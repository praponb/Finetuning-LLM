from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

ranked = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("status")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("amount_per_success"),
    )
    .orderBy("amount_stddev")
)
