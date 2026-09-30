from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy(F.year("opened_date"), "store_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("corr_revenue_units").asc_nulls_last())
w4 = Window.orderBy("greatest_units_share_corr_revenue_units")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-05-23")))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.when(F.col("revenue") >= 3100, "High").when(F.col("revenue") >= 1700, "Medium").otherwise("Low").alias("revenue_tier"),
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)

kept_keys = (
    filtered
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") == 13)
        .select("id", "store_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("units") == 39)
    .select(
        "id",
        "store_id",
        F.corr("revenue", "units").over(w).alias("corr_revenue_units"),
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share_2"),
    )
)

base = (
    metrics
    .filter(F.col("units_share") >= 0.61)
    .select(
        "*",
        F.greatest("units_share", "corr_revenue_units").alias("greatest_units_share_corr_revenue_units"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.corr_revenue_units",
        "e.units_share",
        "e.units_share_2",
        "e.greatest_units_share_corr_revenue_units",
        F.col("p.opened_date"),
        F.col("p.store_name"),
    )
)

enriched_store_sales = (
    enriched
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.sqft"))
)

with_windows = (
    enriched_store_sales
    .withColumn("opened_date_year_min_greatest_units_share_corr_revenue_units", F.min("greatest_units_share_corr_revenue_units").over(w2))
    .withColumn("corr_revenue_units_rank", F.rank().over(w3))
    .withColumn("greatest_units_share_corr_revenue_units_cume_dist", F.cume_dist().over(w4))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("greatest_units_share_corr_revenue_units") >= 79, "LARGE").otherwise("SMALL").alias("greatest_units_share_corr_revenue_units_tier"),
    )
    .agg(
        F.sum("units_share_2").alias("total_units_share_2"),
        F.count("*").alias("num_rows"),
        F.sum("opened_date_year_min_greatest_units_share_corr_revenue_units").alias("total_opened_date_year_min_greatest_units_share_corr_revenue_units"),
        F.round(F.avg("units_share"), 2).alias("avg_units_share"),
        F.max("opened_date_year_min_greatest_units_share_corr_revenue_units").alias("max_opened_date_year_min_greatest_units_share_corr_revenue_units"),
    )
    .orderBy("total_opened_date_year_min_greatest_units_share_corr_revenue_units")
)
