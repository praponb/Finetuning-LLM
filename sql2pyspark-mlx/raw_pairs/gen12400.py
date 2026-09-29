from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("created_at"), "customer_id")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("age_share", "id", "k").rowsBetween(-6, -1)
w4 = Window.partitionBy("customer_id").orderBy(F.col("age").desc(), "id", "k")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "v"),
    )
)

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.credit_limit") > 8400)
    .select("e.id", "e.customer_id", "e.created_at", "e.k", F.col("b.age"))
)

scored = (
    enriched
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "age",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
    )
)

scored_carts = (
    scored
    .select(
        "id",
        "k",
        "age",
        "first_known_k",
        F.avg("age").over(w3).alias("age_avg_6_prior"),
        F.ntile(5).over(w4).alias("age_bucket"),
    )
)

result = (
    scored_carts
    .groupBy(
        F.when(F.col("age_bucket") >= 9, "LARGE").otherwise("SMALL").alias("age_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("k") == "device", F.col("age")).otherwise(0)).alias("device_age"),
        F.percentile_approx("age", 0.9).alias("age_p90"),
        F.count_if(F.col("k") == "weight_kg").alias("weight_kg_events"),
        F.countDistinct("k").alias("distinct_ks"),
        F.round(F.avg("age_avg_6_prior"), 2).alias("avg_age_avg_6_prior"),
        F.count_if(F.col("k") == "utm_source").alias("utm_source_events"),
    )
    .orderBy(F.col("avg_age_avg_6_prior").asc_nulls_last())
)
