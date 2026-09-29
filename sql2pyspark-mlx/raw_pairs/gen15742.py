from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("years_since_sale_date").asc_nulls_last())

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-01").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        (F.datediff(F.current_date(), "sale_date") / 365).alias("years_since_sale_date"),
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_dims = (
    ranked
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.size(F.col("p.tags")) > 2)
    .select("a.*", F.col("p.cost"), F.col("p.price"))
)

kept_keys = (
    with_dims
    .select("id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("units") < 21)
        .select("id", "product_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("product_id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.month("sale_date").isin([1, 4, 11]))
        .select("product_id", "store_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("cost") >= 740, "LARGE").otherwise("SMALL").alias("cost_tier"),
        "store_id",
    )
    .agg(
        F.avg("units").alias("avg_units"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
    )
    .orderBy(F.col("corr_units_revenue").desc_nulls_last())
)
