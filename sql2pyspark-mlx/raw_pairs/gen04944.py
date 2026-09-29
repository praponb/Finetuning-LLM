from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("store_id")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee") > 28)
        .select("store_id")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("year", "order_date").alias("year_start"))
    .agg(
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
    .orderBy(F.col("amount_p50").desc())
)
