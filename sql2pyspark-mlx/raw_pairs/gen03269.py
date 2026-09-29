from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("quarter_start").orderBy(F.col("distinct_channels").desc())

base = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-09-06").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat_ws("/", "city", "region").alias("city_region_key"),
    )
)

cleaned = (
    base
    .filter((F.col("opened_date") >= F.lit("2023-09-03").cast("date")) & (~F.col("region").eqNullSafe(F.lit("West"))))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "city_region_key",
        F.concat("region", F.lit(" "), "store_name").alias("region_store_name"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_last = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel"), F.col("c.discount").alias("order_discount"))
)

attributed = (
    with_last
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("s.opened_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

attributed_stores = (
    attributed
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.opened_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

daily = (
    attributed_stores
    .groupBy(
        F.date_trunc("quarter", "opened_date").alias("quarter_start"),
        "promo_code",
    )
    .agg(
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.max("discount_pct").alias("max_discount_pct"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.sum(F.when(F.col("promotion_promo_code") == "SPRING10", F.col("sqft"))).alias("spring10_total_sqft"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
    )
    .filter(F.col("tokyo_rows") >= 1)
    .withColumn("distinct_channels_rank", F.rank().over(w3))
)

result = (
    daily
    .select(
        "quarter_start",
        "promo_code",
        "distinct_channels",
        "distinct_channels_rank",
    )
    .orderBy(F.col("promo_code").desc(), "distinct_channels_rank")
)
