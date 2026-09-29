from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("created_at")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k")
w3 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("created_at", "id", "k")
w5 = Window.partitionBy("customer_id").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

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

ranked = (
    flattened
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.created_at",
        "a.k",
        "a.v",
        "a.dedup_rank",
        F.col("b.email"),
    )
)

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("created_at") - F.lag("created_at").over(w2) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w3))

flagged = (
    islands
    .withColumn("is_new_session_2", F.when(F.col("created_at") - F.lag("created_at").over(w4) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_carts = flagged.withColumn("burst_id_2", F.sum("is_new_session_2").over(w5))

joined = (
    sessions_carts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.created_at",
        "s.k",
        "s.v",
        "s.dedup_rank",
        "s.is_new_session_2",
        F.col("p.country"),
        F.col("p.email").alias("customer_email"),
    )
)

result = (
    joined
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "dedup_rank",
        "is_new_session_2",
        "customer_email",
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("is_new_session_2").desc_nulls_first())
)
