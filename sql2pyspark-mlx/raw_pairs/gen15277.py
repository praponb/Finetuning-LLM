from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "region").orderBy(F.col("sqft").desc_nulls_first())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("city").orderBy("opened_date", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("opened_date").between("2025-05-01", "2025-08-26"))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
)

scored = (
    valid
    .select(
        "*",
        F.ntile(5).over(w).alias("sqft_bucket"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.lead("sqft", 1).over(w4).alias("next_sqft"),
    )
)

valid_stores = (
    scored
    .filter(F.col("sqft_bucket") < 8)
    .withColumn("store_name_slug", F.regexp_replace("store_name", " ", "-"))
    .withColumn("next_sqft_plus_sqft_share", (F.col("next_sqft") + F.col("sqft_share")))
)

with_last = (
    valid_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.opened_date",
        "s.sqft",
        "s.sqft_bucket",
        "s.sqft_cume_dist",
        "s.sqft_share",
        "s.next_sqft",
        "s.next_sqft_plus_sqft_share",
        F.col("c.channel"),
        F.col("c.amount").alias("order_amount"),
    )
)

matched = (
    with_last
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.opened_date") >= F.col("pr.starts_on")) & (F.col("a.opened_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .groupBy(
        "promo_code",
        F.when(F.col("order_amount") >= 1700, "LARGE").otherwise("SMALL").alias("order_amount_tier"),
    )
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.bit_or("sqft").alias("sqft_bit_or_2"),
        F.count("*").alias("cnt"),
        F.round(F.sum("order_amount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("order_amount_per_store"),
        F.max("sqft_share").alias("max_sqft_share"),
        F.round(F.avg("sqft_cume_dist"), 2).alias("avg_sqft_cume_dist"),
    )
    .orderBy(F.col("order_amount_per_store").desc())
)
