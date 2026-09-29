from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_first(), "id")

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("QUARTER", "start_date").alias("quarter_start"),
        F.date_trunc("MONTH", "end_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.percentile_approx("monthly_fee", 0.5).alias("monthly_fee_p50"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
    )
    .orderBy(F.col("monthly_fee_p90").desc())
)
