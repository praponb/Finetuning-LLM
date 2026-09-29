from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("code").orderBy(F.col("product_id_filled").desc())
w4 = Window.partitionBy(F.month("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy(F.year("created_at"), "customer_id")

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

exploded_carts = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "product_id",
        F.explode("coupon_codes").alias("code"),
    )
)

valid = (
    exploded_carts
    .filter(F.col("code").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "code",
        F.coalesce("product_id", F.lit(0)).alias("product_id_filled"),
        F.hour("created_at").alias("created_at_hour"),
    )
)

kept_keys = (
    valid
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.size("coupon_codes") >= 2)
        .select("id", "customer_id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("product_id_filled") < 28)
    .select(
        "id",
        "customer_id",
        F.min("created_at_hour").over(w).alias("code_min_created_at_hour"),
        F.max("created_at_hour").over(w2).alias("created_at_hour_max_1h"),
        F.percent_rank().over(w3).alias("product_id_filled_percent_rank"),
        F.count("created_at_hour").over(w4).alias("created_at_hour_count_2h"),
        F.avg("created_at_hour").over(w5).alias("created_at_year_avg_created_at_hour"),
    )
)

joined = (
    with_windows
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select(
        "e.customer_id",
        "e.created_at_hour_max_1h",
        "e.product_id_filled_percent_rank",
        "e.created_at_hour_count_2h",
        F.col("b.country"),
    )
)

joined_carts = (
    joined
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.city"))
)

result = (
    joined_carts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["segment", "customer_city", "cart_country"], ["segment", "customer_city"], ["segment"]],
        F.col("c.segment").alias("segment"),
        F.col("c.city").alias("customer_city"),
        F.col("s.country").alias("cart_country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.created_at_hour_count_2h")).alias("min_created_at_hour_count_2h"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("customer_city")
)
