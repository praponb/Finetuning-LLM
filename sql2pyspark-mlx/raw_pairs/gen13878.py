from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("amount", "id", "promo_id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount").desc(), "id", "promo_id")
w3 = Window.partitionBy("promo_code").orderBy("rn", "id", "promo_id")
w4 = Window.orderBy("discount_pct", "id", "promo_id")
w5 = Window.orderBy("prev_discount", "id", "promo_id")
w6 = Window.orderBy(F.col("event_seq").desc_nulls_first())
w7 = Window.orderBy("prev_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("store"))) & F.col("store_id").isNotNull())
    .select("*", F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"))
)

attributed = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

ranked = (
    attributed
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "discount", "promo_id", "promo_code", "discount_pct")
)

ranked_orders = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked_orders
    .select(
        "id",
        "promo_id",
        "discount_pct",
        F.lag("discount_pct").over(w3).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w3).alias("next_discount_pct"),
        F.lead("discount").over(w3).alias("next_discount"),
        F.lag("discount").over(w3).alias("prev_discount"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "promo_id",
        "discount_pct",
        "next_discount_pct",
        "next_discount",
        "event_seq",
        F.row_number().over(w4).alias("event_seq_2"),
        F.row_number().over(w5).alias("seq_no"),
        F.percent_rank().over(w6).alias("event_seq_percent_rank"),
        F.last("prev_discount", ignorenulls=True).over(w7).alias("last_known_prev_discount"),
    )
)

remaining_keys = (
    metrics
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("store_id").isNull())
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("next_discount_tier", F.when(F.col("next_discount") >= 0.39, "PREMIUM").otherwise("STANDARD"))
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.36, "HIGH").otherwise("LOW"))
    .cube("next_discount_tier", "seq_no", "discount_pct_tier")
    .agg(
        F.percentile_approx("promo_id", 0.5).alias("promo_id_p50"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
        F.percentile_approx("promo_id", 0.95).alias("promo_id_p95"),
        F.grouping("next_discount_tier").alias("gid"),
    )
    .filter(F.col("avg_promo_id") > 0)
)
