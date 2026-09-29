from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("customer_id")

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

enriched = (
    flattened
    .filter(F.col("qty") >= 4)
    .select(
        "id",
        "product_id",
        "qty",
        "promo",
        F.avg("qty").over(w).alias("promo_avg_qty"),
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.count("qty").over(w2).alias("customer_id_count_qty"),
        F.min("product_id").over(w).alias("promo_min_product_id"),
    )
)

base = (
    enriched
    .filter(F.col("promo").isNotNull())
    .select(
        "*",
        F.coalesce("customer_id_count_qty", F.lit(0)).alias("customer_id_count_qty_filled"),
        F.round("qty", 2).alias("qty_rounded"),
    )
)

base_carts = (
    base
    .filter(F.col("qty_rounded").between(37, 62))
    .select(
        "id",
        "product_id",
        "qty",
        "promo_avg_qty",
        "product_id_share",
        "qty_share",
        "customer_id_count_qty",
        "promo_min_product_id",
        "customer_id_count_qty_filled",
        "qty_rounded",
        F.when(F.col("customer_id_count_qty") >= 16, "High").when(F.col("customer_id_count_qty") >= 5, "Medium").otherwise("Low").alias("customer_id_count_qty_level"),
        (F.col("customer_id_count_qty_filled") - F.col("qty_share")).alias("customer_id_count_qty_filled_minus_qty_share"),
        F.coalesce("promo", F.lit("DIRECT")).alias("promo_filled"),
    )
)

result = (
    base_carts
    .groupBy(
        F.when(F.col("customer_id_count_qty") >= 16, "LARGE").otherwise("SMALL").alias("customer_id_count_qty_tier"),
        F.when(F.col("qty_share") >= 0.67, "HIGH").otherwise("LOW").alias("qty_share_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("qty_rounded", 0.9).alias("qty_rounded_p90"),
    )
    .orderBy(F.col("qty_rounded_p90").asc_nulls_last())
)
