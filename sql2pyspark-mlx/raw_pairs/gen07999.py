from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-05-07").cast("date")) & (F.col("revenue") < 1600))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)

base_store_sales = (
    base
    .filter((F.col("sale_date") >= F.lit("2023-03-20").cast("date")) & F.col("revenue_per_unit").between(46, 75))
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "revenue_per_unit",
        (F.col("revenue_per_unit") * F.col("units")).alias("revenue_per_unit_times_units"),
    )
)

latest = (
    base_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .filter(F.col("product_id") == 4)
    .select(
        "id",
        "store_id",
        "revenue",
        "revenue_per_unit",
        "revenue_per_unit_times_units",
        "latest_rank",
        F.lead("revenue").over(w2).alias("next_revenue"),
        F.lag("revenue_per_unit_times_units").over(w2).alias("prev_revenue_per_unit_times_units"),
        F.lead("revenue_per_unit_times_units").over(w2).alias("next_revenue_per_unit_times_units"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

kept_keys = (
    with_windows
    .select("store_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("revenue").isNotNull())
        .select("store_id", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .rollup(
        "store_id",
        F.when(F.col("next_revenue") >= 3500, "HIGH").otherwise("LOW").alias("next_revenue_tier"),
    )
    .agg(
        F.percentile_approx("next_revenue_per_unit_times_units", 0.5).alias("next_revenue_per_unit_times_units_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("revenue_per_unit", [0.25, 0.5, 0.75]).alias("revenue_per_unit_quartiles"),
    )
    .orderBy("store_id")
)
