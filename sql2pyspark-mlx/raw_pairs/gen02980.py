from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id")
w_ordered = Window.partitionBy("customer_id").orderBy("product_id")

lines = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

filtered = (
    lines
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "product_id",
        "price",
        F.ceil("qty").alias("qty_whole"),
        (F.col("qty") - F.col("product_id")).alias("qty_minus_product_id"),
        F.coalesce("promo", F.lit("OTHER")).alias("resolved_promo"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.credit_limit").between(12600, 14900))
    .select(
        "src.id",
        "src.customer_id",
        "src.product_id",
        "src.resolved_promo",
        F.col("p.signup_date"),
        F.col("p.credit_limit"),
        F.col("p.age"),
    )
)

metrics = (
    enriched
    .select(
        "resolved_promo",
        "credit_limit",
        "age",
        F.max("credit_limit").over(w_running).alias("customer_id_max_credit_limit"),
        F.count("age").over(w_running).alias("customer_id_count_age"),
        F.percent_rank().over(w_ordered).alias("product_id_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("product_id_percent_rank").between(0.6, 0.75))
    .select(
        "*",
        F.col("customer_id_max_credit_limit").cast("double").alias("customer_id_max_credit_limit_str"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("customer_id_count_age") >= 14, "PREMIUM").otherwise("STANDARD").alias("customer_id_count_age_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum("customer_id_max_credit_limit_str").alias("total_customer_id_max_credit_limit_str"),
        F.percentile_approx("customer_id_count_age", 0.5).alias("customer_id_count_age_p50"),
        F.percentile_approx("customer_id_max_credit_limit", [0.25, 0.5, 0.75]).alias("customer_id_max_credit_limit_quartiles"),
    )
)
