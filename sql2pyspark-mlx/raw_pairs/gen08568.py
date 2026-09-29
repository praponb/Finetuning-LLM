from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc_nulls_first())
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy(F.to_date("created_at"), "segment")
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

lines = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("li_item.product_id"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

joined = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.customer_id",
        "e.created_at",
        "e.promo",
        F.col("d.name"),
        F.col("d.age"),
        F.col("d.segment"),
    )
)

metrics = (
    joined
    .filter(F.col("name").isNotNull())
    .select(
        "customer_id",
        "created_at",
        F.cume_dist().over(w).alias("age_cume_dist"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.count("age").over(w3).alias("created_at_to_date_count_age"),
        F.sum("age").over(w4).alias("age_sum_2h"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share_2"),
    )
)

base = (
    metrics
    .filter(F.col("customer_id").isNotNull())
    .withColumn("age_cume_dist_times_created_at_to_date_count_age", (F.col("age_cume_dist") * F.col("created_at_to_date_count_age")))
)

joined_carts = (
    base
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.signup_date"))
)

result = (
    joined_carts
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "age_cume_dist",
        "age_share",
        "created_at_to_date_count_age",
        "age_share_2",
        F.col("min_order_shipping_fee"),
    )
    .orderBy("customer_id")
)
