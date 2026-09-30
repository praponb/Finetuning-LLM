from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("order_date").desc())
w2 = Window.partitionBy("city").orderBy("rn", "id")
w3 = Window.partitionBy("city").orderBy(F.col("age").desc())
w4 = Window.partitionBy("store_id").orderBy("age_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    latest
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.shipping_fee",
        "s.rn",
        F.col("c.segment"),
        F.col("c.age"),
        F.col("c.city"),
    )
)

scored = (
    with_dims
    .select(
        "*",
        F.lag("shipping_fee").over(w2).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w2).alias("next_shipping_fee"),
        F.lag("age").over(w2).alias("prev_age"),
        F.lead("age").over(w2).alias("next_age"),
        F.row_number().over(w2).alias("event_seq"),
        F.dense_rank().over(w3).alias("age_dense_rank"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "status",
        "shipping_fee",
        "segment",
        "city",
        "prev_age",
        "age_dense_rank",
        F.sum("event_seq").over(w4).alias("running_sum_event_seq"),
        F.max("event_seq").over(w4).alias("running_max_event_seq"),
        F.avg("event_seq").over(w4).alias("running_avg_event_seq"),
    )
)

result = (
    metrics
    .withColumn("prev_age_tier", F.when(F.col("prev_age") >= 70, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["prev_age_tier", "status"], ["prev_age_tier"]],
        "prev_age_tier",
        "status",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("running_avg_event_seq") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("running_avg_event_seq_per_chiang_mai"),
        F.percentile_approx("running_avg_event_seq", 0.5).alias("running_avg_event_seq_p50"),
    )
    .orderBy(F.col("running_avg_event_seq_per_chiang_mai").desc())
)
