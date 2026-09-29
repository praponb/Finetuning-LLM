from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("customer_id").orderBy(F.col("is_active").desc(), "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("customer_id", "country").orderBy(F.col("is_active").desc())
w5 = Window.partitionBy("country", "status").orderBy("country_stddev_samp_is_active", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("monthly_fee", "id")
w7 = Window.partitionBy("customer_id").orderBy("country_stddev_samp_is_active", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("is_active").desc(), "id")
w9 = Window.partitionBy("customer_id").orderBy("is_active_rank", "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") > F.to_date(F.lit("2023-10-09"))))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.when(F.col("status") == "ACTIVE", 1).otherwise(0).alias("is_active"),
        F.trim("status").alias("clean_status"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.monthly_fee",
        "e.status",
        "e.is_active",
        F.col("c.name"),
        F.col("c.country"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.max("is_active").over(w).alias("status_max_is_active"),
        F.ntile(5).over(w2).alias("is_active_bucket"),
        F.stddev_samp("is_active").over(w3).alias("country_stddev_samp_is_active"),
        F.rank().over(w4).alias("is_active_rank"),
    )
)

with_windows_subscriptions = (
    with_windows
    .filter(F.col("monthly_fee") >= 70)
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w5))
    .withColumn("prev_country_stddev_samp_is_active", F.lag("country_stddev_samp_is_active", 1).over(w6))
    .withColumn("first_known_is_active_rank", F.first("is_active_rank", ignorenulls=True).over(w7))
    .withColumn("is_active_bucket_2", F.ntile(4).over(w8))
    .withColumn("is_active_rank_bucket", F.ntile(10).over(w9))
)

result = (
    with_windows_subscriptions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .cube(F.col("c.segment").alias("segment"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.is_active")), 2).alias("total_is_active"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("total_is_active").asc_nulls_first())
)
