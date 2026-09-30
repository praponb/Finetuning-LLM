from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

base = (
    flattened
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 15:00:00"))) & F.col("promo").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "promo",
        F.array_join("coupon_codes", "|").alias("coupon_codes_text"),
    )
)

valid = (
    base
    .filter(F.col("created_at") >= "2025-02-20")
    .withColumn("qty_minus_product_id", (F.col("qty") - F.col("product_id")))
    .withColumn("created_at_date", F.to_date("created_at"))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("N/A")))
)

scored = (
    valid
    .withColumn("promo_avg_qty_minus_product_id", F.avg("qty_minus_product_id").over(w))
)

enriched = (
    scored
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.age"))
)

result = (
    enriched
    .cube(F.date_trunc("YEAR", "created_at_date").alias("year_start"), "customer_id")
    .agg(
        F.avg("promo_avg_qty_minus_product_id").alias("avg_promo_avg_qty_minus_product_id"),
        F.round(F.sum("qty_minus_product_id") / F.nullif(F.count(F.when(F.col("promo") == "WELCOME5", 1)), F.lit(0)), 2).alias("qty_minus_product_id_per_welcome5"),
        F.max("created_at").alias("ended_at"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
