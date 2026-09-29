from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel").orderBy("running_max_discount_pct", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("running_avg_discount_pct", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("running_avg_discount_pct", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .filter(F.col("discount_pct") < 0.16)
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.max("discount_pct").over(w_part).alias("running_max_discount_pct"),
        F.avg("discount_pct").over(w_part).alias("running_avg_discount_pct"),
        F.min("discount_pct").over(w_part).alias("running_min_discount_pct"),
    )
)

metrics_promotions = (
    metrics
    .withColumn("last4_count_running_max_discount_pct", F.count("running_max_discount_pct").over(w_grp))
    .withColumn("last4_avg_running_max_discount_pct", F.avg("running_max_discount_pct").over(w_grp))
    .withColumn("last4_max_running_max_discount_pct", F.max("running_max_discount_pct").over(w_grp))
    .withColumn("last4_min_running_max_discount_pct", F.min("running_max_discount_pct").over(w_grp))
    .withColumn("running_avg_discount_pct_avg_2_prior", F.avg("running_avg_discount_pct").over(w3))
)

valid = (
    metrics_promotions
    .filter(F.col("channel").isNotNull())
    .select("*", F.last_day("starts_on").alias("starts_on_month_end"))
)

enriched = (
    valid
    .select(
        "id",
        "starts_on",
        "running_min_discount_pct",
        "last4_avg_running_max_discount_pct",
        "last4_max_running_max_discount_pct",
        "running_avg_discount_pct_avg_2_prior",
        F.avg("last4_avg_running_max_discount_pct").over(w4).alias("last4_avg_running_max_discount_pct_avg_2"),
        F.last("last4_avg_running_max_discount_pct", ignorenulls=True).over(w5).alias("last_known_last4_avg_running_max_discount_pct"),
    )
)

result = (
    enriched
    .select(
        "id",
        "starts_on",
        "last4_max_running_max_discount_pct",
        "running_avg_discount_pct_avg_2_prior",
        "last4_avg_running_max_discount_pct_avg_2",
        "last_known_last4_avg_running_max_discount_pct",
    )
    .orderBy(F.col("running_avg_discount_pct_avg_2_prior").asc_nulls_last())
)
