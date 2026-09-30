from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc_nulls_first())
w_grp = Window.partitionBy("region", "city")
w3 = Window.orderBy(F.col("sqft").desc())
w4 = Window.partitionBy("city", "region").orderBy("sqft")
w5 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())
w6 = Window.partitionBy("region", "city").orderBy("sqft")
w7 = Window.partitionBy("city").orderBy("sqft_percent_rank")
w8 = Window.orderBy(F.col("sqft_rank_2").desc_nulls_first())
w9 = Window.partitionBy("region").orderBy(F.col("sqft_rank").desc_nulls_first())

matched_keys = (
    df_stores
    .select("city", "id")
    .intersectAll(
        df_stores
        .filter(~F.col("region").isin(["East", "West"]))
        .select("city", "id")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_anti")
)

with_windows = (
    kept
    .select(
        "*",
        F.dense_rank().over(w).alias("sqft_dense_rank"),
        F.avg("sqft").over(w_grp).alias("region_avg_sqft"),
        F.percent_rank().over(w3).alias("sqft_percent_rank"),
        F.rank().over(w4).alias("sqft_rank"),
        F.rank().over(w5).alias("sqft_rank_2"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.rank().over(w6).alias("sqft_rank_3"),
        F.dense_rank().over(w7).alias("sqft_percent_rank_dense_rank"),
        F.dense_rank().over(w8).alias("sqft_rank_2_dense_rank"),
        F.rank().over(w9).alias("sqft_rank_rank"),
    )
)

cleaned = (
    enriched
    .filter((F.col("opened_date") > F.lit("2023-07-22").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_dense_rank",
        "region_avg_sqft",
        "sqft_rank",
        "sqft_rank_2",
        "sqft_rank_3",
        "sqft_percent_rank_dense_rank",
        "sqft_rank_2_dense_rank",
        "sqft_rank_rank",
        F.round("sqft_rank_3", 0).alias("sqft_rank_3_rounded"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.min("sqft_rank").alias("min_sqft_rank"),
        F.sum("sqft_rank_3").alias("total_sqft_rank_3"),
        F.countDistinct("city").alias("distinct_cities"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
)
