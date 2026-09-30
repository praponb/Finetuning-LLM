from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc_nulls_first())
w2 = Window.partitionBy("tag").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy(F.to_date("created_at"), "customer_city")
w4 = Window.partitionBy("city", "customer_id").orderBy(F.col("customer_age").desc_nulls_last())
w5 = Window.partitionBy("tag").orderBy(F.col("age").asc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

enriched = (
    exploded
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.email").isNull())
    .select("src.*", F.col("b.credit_limit"), F.col("b.age"), F.col("b.city"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.*",
        F.col("c.city").alias("customer_city"),
        F.col("c.age").alias("customer_age"),
        F.col("c.email"),
    )
)

scored = (
    joined
    .select(
        "customer_id",
        "created_at",
        "tag",
        "age",
        "city",
        "email",
        F.rank().over(w).alias("age_rank"),
        F.sum("customer_age").over(w2).alias("customer_age_sum_30m"),
        F.round((F.col("customer_age") / F.sum("customer_age").over(w3)), 4).alias("customer_age_share"),
        F.rank().over(w4).alias("customer_age_rank"),
        F.cume_dist().over(w5).alias("age_cume_dist"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.email").alias("customer_email"))
)

remaining_keys = (
    with_dims
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.size("coupon_codes") >= 2)
        .select("customer_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .groupBy("city")
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.countDistinct("tag").alias("distinct_tags"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
)

result = (
    grouped
    .select("city", "span_minutes")
    .orderBy(F.col("city").desc_nulls_first())
    .limit(5)
)
