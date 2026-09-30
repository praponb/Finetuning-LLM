from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promo").orderBy(F.col("credit_limit").desc())
w2 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_running = Window.partitionBy("customer_id")

flattened = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.promo"),
    )
)

joined = (
    flattened
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select(
        "e.customer_id",
        "e.created_at",
        "e.coupon_codes",
        "e.promo",
        F.col("b.credit_limit"),
    )
)

metrics = (
    joined
    .filter(F.col("customer_id").isin([3, 12, 13, 31]))
    .withColumn("credit_limit_percent_rank", F.percent_rank().over(w_part))
    .withColumn("credit_limit_sum_15m", F.sum("credit_limit").over(w2))
    .withColumn("customer_id_min_credit_limit", F.min("credit_limit").over(w_running))
)

enriched = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.name"),
        F.col("b.credit_limit").alias("customer_credit_limit"),
    )
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("credit_limit_sum_15m") >= 76220, "LARGE").otherwise("SMALL").alias("credit_limit_sum_15m_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        "customer_id",
    )
    .agg(
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.size(F.collect_set("promo")).alias("num_promos_2"),
        F.max("created_at").alias("last_created_at"),
        F.percentile_approx("credit_limit_sum_15m", 0.5).alias("credit_limit_sum_15m_p50"),
    )
    .filter(F.col("credit_limit_sum_15m_p50") > 0)
)

result = (
    grouped
    .select(
        "credit_limit_sum_15m_tier",
        "activity_day",
        "customer_id",
        "last_created_at",
    )
    .orderBy(F.col("activity_day").desc(), "last_created_at")
)
