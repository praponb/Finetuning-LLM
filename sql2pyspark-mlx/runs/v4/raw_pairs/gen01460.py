from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("start_date").desc(), "id")

ranked = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("YEAR", "start_date").alias("year_start"),
        F.when(F.col("monthly_fee") >= 224, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .orderBy("year_start")
)
