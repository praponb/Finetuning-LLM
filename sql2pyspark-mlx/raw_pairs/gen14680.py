from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("subscription_status"))
)

result = (
    with_last
    .groupBy("country")
    .agg(
        F.percentile_approx("age", 0.9).alias("age_p90"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("subscription_status") == "PAUSED", 1)).alias("paused_rows"),
    )
    .orderBy(F.col("distinct_cities").desc_nulls_first())
)
