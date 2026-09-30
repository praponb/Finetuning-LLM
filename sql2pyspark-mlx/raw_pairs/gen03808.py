from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("store_id")
w_ordered = Window.orderBy("sqft", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy(F.year("sale_date"), "store_id")

valid = (
    df_store_sales
    .filter(F.col("product_id") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.floor("revenue").alias("revenue_whole"),
    )
)

with_dims = (
    valid
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.sale_date",
        "s.revenue",
        F.col("p.store_name"),
        F.col("p.sqft"),
    )
)

metrics = (
    with_dims
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_5"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
        F.avg("revenue").over(w_ordered).alias("revenue_avg_3_prior"),
    )
)

with_dims_store_sales = (
    metrics
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.sqft").alias("store_sqft"))
)

matched_keys = (
    with_dims_store_sales
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id") == 10)
        .select("id", "store_id")
    )
)

verified = (
    with_dims_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "id",
        "sale_date",
        "store_name",
        "sqft",
        "store_sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.round((F.col("revenue") / F.sum("revenue").over(w4)), 4).alias("revenue_share_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        F.when(F.col("sqft") >= 13997, "HIGH").otherwise("LOW").alias("sqft_tier"),
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("store_sqft", [0.25, 0.5, 0.75]).alias("store_sqft_quartiles"),
        F.percentile_approx("sqft_share", [0.25, 0.5, 0.75]).alias("sqft_share_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
