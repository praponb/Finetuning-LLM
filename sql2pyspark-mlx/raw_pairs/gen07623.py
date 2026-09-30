from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.year("sale_date"), "store_id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy(F.date_trunc("WEEK", "sale_date"), "product_id")
w4 = Window.partitionBy("store_id", "region")
w5 = Window.partitionBy("store_id").orderBy(F.col("units_share_3").desc())
w6 = Window.partitionBy(F.month("sale_date"), "store_id")
w7 = Window.orderBy(F.col("sale_date_year_max_revenue").desc())
w8 = Window.orderBy(F.col("units_share_2").desc_nulls_last())
w9 = Window.partitionBy("store_id")
w10 = Window.partitionBy(F.date_trunc("WEEK", "sale_date"))
w11 = Window.partitionBy("region", "store_id")

joined = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.region"))
)

matched_keys = (
    joined
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") <= "2025-09-23")
        .select("id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "region",
        F.max("revenue").over(w_hist).alias("sale_date_year_max_revenue"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.round((F.col("units") / F.sum("units").over(w3)), 4).alias("units_share_2"),
        F.round((F.col("units") / F.sum("units").over(w4)), 4).alias("units_share_3"),
    )
)

metrics = (
    scored
    .withColumn("units_share_3_percent_rank", F.percent_rank().over(w5))
    .withColumn("sale_date_month_stddev_samp_sale_date_year_max_revenue", F.stddev_samp("sale_date_year_max_revenue").over(w6))
    .withColumn("sale_date_year_max_revenue_rank", F.rank().over(w7))
)

with_windows = (
    metrics
    .filter(F.col("sale_date_month_stddev_samp_sale_date_year_max_revenue") >= 1337)
    .select(
        "*",
        F.round((F.col("sale_date_month_stddev_samp_sale_date_year_max_revenue") / F.sum("sale_date_month_stddev_samp_sale_date_year_max_revenue").over(w2)), 4).alias("sale_date_month_stddev_samp_sale_date_year_max_revenue_share"),
        F.rank().over(w8).alias("units_share_2_rank"),
        F.min("units_share_2").over(w9).alias("store_id_min_units_share_2"),
        F.round((F.col("sale_date_year_max_revenue") / F.sum("sale_date_year_max_revenue").over(w10)), 4).alias("sale_date_year_max_revenue_share"),
        F.round((F.col("sale_date_year_max_revenue_rank") / F.sum("sale_date_year_max_revenue_rank").over(w11)), 4).alias("sale_date_year_max_revenue_rank_share"),
    )
)

result = (
    with_windows
    .alias("src")
    .join(df_stores.alias("dim"), F.col("src.store_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "store_region", "store_sale_region"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("dim.region").alias("store_region"),
        F.col("src.region").alias("store_sale_region"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.units_share")).alias("total_units_share"),
        F.max(F.col("src.store_id_min_units_share_2")).alias("max_store_id_min_units_share_2"),
    )
)
