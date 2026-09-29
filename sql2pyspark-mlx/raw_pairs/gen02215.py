from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)

metrics = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 130)
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w_ordered).alias("last3_sum_monthly_fee"),
        F.max("monthly_fee").over(w_ordered).alias("last3_max_monthly_fee"),
        F.min("monthly_fee").over(w_ordered).alias("last3_min_monthly_fee"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last3_max_monthly_fee") >= 223, "LARGE").otherwise("SMALL").alias("last3_max_monthly_fee_tier"),
    )
    .agg(
        F.min("last3_min_monthly_fee").alias("min_last3_min_monthly_fee"),
        F.percentile_approx("last3_min_monthly_fee", 0.9).alias("last3_min_monthly_fee_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
