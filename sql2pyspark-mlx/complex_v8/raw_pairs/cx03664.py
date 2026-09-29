from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_group = Window.partitionBy("city_group")
w_city_group_city_by_capacity = Window.partitionBy("city_group", "city").orderBy("capacity_filled", "id")
w_city_group_city_by_capacity_2 = Window.partitionBy("city_group", "city").orderBy(F.col("capacity_filled").desc(), "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_warehouses
    .filter((F.col("city") != "Singapore") & (F.col("opened_on") >= F.lit("2024-04-13").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.concat(F.col("city"), F.lit("#"), F.col("id").cast("string")).alias("city_ref"),
    )
)

annotated = (
    filtered
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "city_ref",
        F.substring(F.trim(F.col("wh_code")), 1, 3).alias("wh_code_prefix"),
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.concat_ws("-", F.col("wh_code"), F.col("city_ref")).alias("wh_code_city_ref_label"),
    )
)

derived = (
    annotated
    .filter(F.col("capacity_filled") > 3850)
    .select(
        "*",
        F.when(F.col("city").isin("Kuala Lumpur", "Bangkok"), "group_a").when(F.col("city") == "Singapore", "group_b").otherwise("other").alias("city_group"),
        (F.col("capacity") + F.col("capacity_filled")).alias("capacity_plus_capacity_filled"),
    )
)

ranked = (
    derived
    .select(
        "id",
        "city",
        "capacity",
        "city_ref",
        "wh_code_prefix",
        "capacity_filled",
        "city_group",
        F.count(F.lit(1)).over(w_city_group).alias("rows_in_city_group"),
        F.ntile(5).over(w_city_group_city_by_capacity).alias("capacity_filled_quintile"),
        (F.col("capacity") - F.lag("capacity").over(w_city_group_city_by_capacity)).alias("capacity_change"),
        F.dense_rank().over(w_city_group_city_by_capacity).alias("capacity_filled_dense_rank"),
        F.round(F.col("capacity_filled") - F.avg("capacity_filled").over(w_city_group), 2).alias("capacity_filled_vs_city_group_avg"),
        F.round(F.avg("capacity_filled").over(w_city_group_city_by_capacity_2), 2).alias("capacity_filled_avg_4"),
    )
)

result = (
    ranked
    .cube("capacity_filled_quintile", "city_group")
    .agg(
        F.countDistinct("city_group").alias("distinct_city_groups"),
        F.count("*").alias("row_count"),
        F.sum("capacity_filled_avg_4").alias("total_capacity_filled_avg_4"),
    )
    .orderBy(
        F.col("capacity_filled_quintile").asc_nulls_last(),
        F.col("city_group").asc_nulls_last(),
    )
)
