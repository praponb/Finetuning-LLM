from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_last())
w3 = Window.orderBy(F.col("sqft_rounded").asc_nulls_first())
w4 = Window.partitionBy(F.year("sale_date"), "region")
w5 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())
w6 = Window.partitionBy("city").orderBy(F.col("sqft_rank").desc())
w7 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc_nulls_first())
w8 = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w9 = Window.partitionBy("city")
w10 = Window.partitionBy("region")

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-04-29"))) & (F.col("region").eqNullSafe(F.lit("Central"))))
    .select("*", F.round("sqft", 1).alias("sqft_rounded"))
)

with_last = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

matched_keys = (
    with_last
    .select("id", "city")
    .intersectAll(
        df_stores
        .filter(F.col("sqft") > 15644)
        .select("id", "city")
    )
)

verified = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .filter(F.col("city").isin(["Tokyo", "Singapore"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.rank().over(w2).alias("sqft_rank"),
        F.percent_rank().over(w3).alias("sqft_rounded_percent_rank"),
        F.count("sqft").over(w4).alias("sale_date_year_count_sqft"),
        F.dense_rank().over(w5).alias("sqft_dense_rank"),
    )
)

metrics_stores = (
    metrics
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "sqft_rounded_percent_rank",
        F.dense_rank().over(w6).alias("sqft_rank_dense_rank"),
        F.cume_dist().over(w7).alias("sqft_cume_dist"),
        F.cume_dist().over(w8).alias("sqft_cume_dist_2"),
        F.round((F.col("sqft_dense_rank") / F.sum("sqft_dense_rank").over(w9)), 4).alias("sqft_dense_rank_share"),
        F.avg("sqft_rounded_percent_rank").over(w9).alias("city_avg_sqft_rounded_percent_rank"),
    )
)

enriched = (
    metrics_stores
    .select(
        "store_name",
        "region",
        "sqft_rank_dense_rank",
        "sqft_cume_dist",
        "sqft_cume_dist_2",
        "sqft_dense_rank_share",
        "city_avg_sqft_rounded_percent_rank",
        F.sum("sqft_rounded_percent_rank").over(w10).alias("region_sum_sqft_rounded_percent_rank"),
        F.max("sqft_rank_dense_rank").over(w10).alias("region_max_sqft_rank_dense_rank"),
        F.sum("sqft_cume_dist_2").over(w10).alias("region_sum_sqft_cume_dist_2"),
    )
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("region_sum_sqft_rounded_percent_rank") >= 4, "HIGH").otherwise("LOW").alias("region_sum_sqft_rounded_percent_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sort_array(F.collect_set("region")).alias("region_list_2"),
        F.sort_array(F.collect_set("region")).alias("region_list_3"),
    )
)

result = (
    grouped
    .select(
        "region_sum_sqft_rounded_percent_rank_tier",
        "region_list_2",
        "region_list_3",
    )
)
