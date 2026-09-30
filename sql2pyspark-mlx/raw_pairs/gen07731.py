from pyspark.sql import functions as F

matched_keys = (
    df_inventory
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("quantity").between(130, 161))
        .select("product_id", "id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("YEAR", "last_restock").alias("year_start"))
    .agg(
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.count("*").alias("cnt"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
    )
    .orderBy(F.col("avg_quantity").desc_nulls_last())
)
