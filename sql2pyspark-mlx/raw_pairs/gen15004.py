from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("credit_limit").desc(), "id", "key")
w2 = Window.partitionBy("segment", "key").orderBy("age", "id", "key").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("segment", "key").orderBy("latest_rank", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("last4_min_age", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("key").orderBy("credit_limit", "id", "key")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

with_dims = (
    exploded
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.age").between(41, 62))
    .select(
        "src.id",
        "src.customer_id",
        "src.coupon_codes",
        "src.key",
        F.col("p.credit_limit"),
        F.col("p.segment"),
        F.col("p.country"),
    )
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims_carts = (
    ranked
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city"), F.col("d.name"), F.col("d.age"))
)

with_windows = (
    with_dims_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "key",
        "credit_limit",
        "segment",
        "country",
        "latest_rank",
        "city",
        "name",
        F.avg("age").over(w2).alias("last4_avg_age"),
        F.sum("age").over(w2).alias("last4_sum_age"),
        F.min("age").over(w2).alias("last4_min_age"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "coupon_codes",
        "key",
        "credit_limit",
        "city",
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
        F.last("last4_min_age", ignorenulls=True).over(w4).alias("last_known_last4_min_age"),
        F.lead("credit_limit", 1).over(w5).alias("next_credit_limit"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("last_known_last4_min_age") >= 66, "LARGE").otherwise("SMALL").alias("last_known_last4_min_age_tier"),
        F.when(F.col("credit_limit") >= 16800, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
        "city",
    )
    .agg(
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_count"),
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit")).otherwise(0)).alias("new_york_credit_limit"),
        F.avg(F.when(F.col("key") == "weight_kg", F.col("next_credit_limit"))).alias("weight_kg_avg_next_credit_limit"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("last_known_last4_min_age"))).alias("chiang_mai_total_last_known_last4_min_age"),
    )
)
