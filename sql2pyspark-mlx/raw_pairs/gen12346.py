from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w_grp = Window.partitionBy("country", "segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment")

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .withColumn("clean_name", F.trim("name"))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("N/A")))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "clean_name",
        "segment_filled",
    )
)

metrics = (
    ranked
    .select(
        "id",
        "name",
        "is_active",
        "credit_limit",
        "segment_filled",
        F.max("credit_limit").over(w_grp).alias("running_max_credit_limit"),
        F.count("credit_limit").over(w_grp).alias("running_count_credit_limit"),
        F.min("credit_limit").over(w_grp).alias("running_min_credit_limit"),
        F.avg("credit_limit").over(w_grp).alias("running_avg_credit_limit"),
        F.stddev_samp("credit_limit").over(w3).alias("segment_stddev_samp_credit_limit"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.shipping_fee") > 12), "left_anti")
)

stats = (
    not_flagged
    .groupBy(
        F.when(F.col("segment_stddev_samp_credit_limit") >= 17648, "LARGE").otherwise("SMALL").alias("segment_stddev_samp_credit_limit_tier"),
        F.when(F.col("running_count_credit_limit") >= 18, "HIGH").otherwise("LOW").alias("running_count_credit_limit_tier"),
    )
    .agg(
        F.percentile_approx("segment_stddev_samp_credit_limit", [0.25, 0.5, 0.75]).alias("segment_stddev_samp_credit_limit_quartiles"),
        F.round(F.avg("running_avg_credit_limit"), 2).alias("avg_running_avg_credit_limit"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
    )
)

result = (
    stats
    .select(
        "segment_stddev_samp_credit_limit_tier",
        "running_count_credit_limit_tier",
        "avg_running_avg_credit_limit",
    )
)
