from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("device").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("age", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("device")
w_hist = Window.partitionBy("device").orderBy("running_max_age_avg_3_prior", "id")
w6 = Window.partitionBy("device").orderBy("age_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_latest = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.email", "s.is_active", "s.age", F.col("c.device"))
)

with_windows = (
    with_latest
    .select(
        "*",
        F.last("device", ignorenulls=True).over(w_ordered).alias("last_known_device"),
        F.avg("age").over(w_running).alias("age_avg_3_prior"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "email",
        "is_active",
        "age",
        "device",
        "age_avg_3_prior",
        F.count("age_avg_3_prior").over(w_ordered).alias("running_count_age_avg_3_prior"),
        F.avg("age_avg_3_prior").over(w_ordered).alias("running_avg_age_avg_3_prior"),
        F.max("age_avg_3_prior").over(w_ordered).alias("running_max_age_avg_3_prior"),
    )
)

enriched_customers = (
    enriched
    .filter(F.lower("email").like("%ack%"))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w4)), 4))
    .withColumn("next_running_avg_age_avg_3_prior", F.lead("running_avg_age_avg_3_prior", 1).over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("first_known_device", F.first("device", ignorenulls=True).over(w6))
)

result = (
    enriched_customers
    .groupBy("device")
    .agg(
        F.count("*").alias("row_count"),
        F.avg(F.when(F.col("device") == "tablet", F.col("age_avg_3_prior"))).alias("tablet_avg_age_avg_3_prior"),
        F.percentile_approx("next_running_avg_age_avg_3_prior", [0.25, 0.5, 0.75]).alias("next_running_avg_age_avg_3_prior_quartiles"),
    )
    .filter(F.col("row_count") > 1)
)
