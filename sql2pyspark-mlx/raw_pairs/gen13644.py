from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")

ranked = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "monthly_fee")
)

result = (
    ranked
    .groupBy("customer_id")
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
    )
)
