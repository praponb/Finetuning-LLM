from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("launch_date_label", F.date_format("launch_date", "yyyy-MM"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("NONE")))
)

with_last = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.month(F.col("ch.last_restock")).isin([7, 8, 11])), "left_semi")
)

remaining_keys = (
    not_flagged
    .select("supplier_id", "id")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isNotNull())
        .select("supplier_id", "id")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"), "supplier_id")
    .agg(
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.round(F.avg("store_sale_revenue"), 2).alias("avg_store_sale_revenue"),
        F.round(F.sum("store_sale_revenue") / F.nullif(F.count(F.when(F.col("category") == "Sports", 1)), F.lit(0)), 2).alias("store_sale_revenue_per_sports"),
    )
    .filter(F.col("avg_store_sale_revenue") > 0)
)
