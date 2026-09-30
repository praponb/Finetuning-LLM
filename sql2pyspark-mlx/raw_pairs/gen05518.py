from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc_nulls_first(), "id")
w2 = Window.partitionBy("status", "promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("order_date"), "promo_code").orderBy("amount")
w4 = Window.partitionBy("channel").orderBy("discount", "id", "promo_id").rowsBetween(-2, -1)

ranked = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

attributed = (
    ranked
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    attributed
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "row_num",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
        F.rank().over(w3).alias("amount_rank"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_2_prior"),
    )
)

cleaned = (
    metrics
    .filter((F.col("order_date") >= F.lit("2023-03-21").cast("date")) & (~F.col("promo_code").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "row_num",
        "promo_id",
        "promo_code",
        "discount_pct",
        "amount_rank",
        "discount_pct_avg_2_prior",
        F.when(F.col("discount_pct") >= 0.28, "High").when(F.col("discount_pct") >= 0.1, "Medium").otherwise("Low").alias("discount_pct_level"),
        F.coalesce("promo_code", F.lit("NONE")).alias("resolved_promo_code"),
    )
)

result = (
    cleaned
    .groupBy("promo_code", "promo_id")
    .agg(
        F.percentile_approx("discount_pct_avg_2_prior", 0.9).alias("discount_pct_avg_2_prior_p90"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("amount_rank")).otherwise(0)).alias("bundle_amount_rank"),
    )
    .filter(F.col("bundle_amount_rank") > 0)
    .orderBy(F.col("distinct_customer_ids").desc_nulls_last())
)
