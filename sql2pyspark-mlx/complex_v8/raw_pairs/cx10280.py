from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_site_id_by_kwh_d = Window.partitionBy("site_tariff", "site_id").orderBy("kwh_distance", "id")
w_site_id = Window.partitionBy("site_id")
w_site_tariff_site_id_by_kwh_d_2 = Window.partitionBy("site_tariff", "site_id").orderBy("kwh_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_site_tariff_by_kwh_d = Window.partitionBy("site_id", "site_tariff").orderBy(F.col("kwh_distance").desc_nulls_first(), "id")
w_site_id_site_tariff_by_kwh_d_2 = Window.partitionBy("site_id", "site_tariff").orderBy(F.col("kwh_distance").desc_nulls_first())
w_rn_site_tariff_group = Window.partitionBy("rn", "site_tariff_group")
w_all_by_next_kwh_distance_ful = Window.orderBy(F.col("next_kwh_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_kwh_distance = Window.orderBy(F.col("next_kwh_distance").desc(), "id")
w_all_by_next_kwh_distance_run = Window.orderBy(F.col("next_kwh_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_site_id_by_next_kwh_dista = Window.partitionBy("rn", "site_id").orderBy("next_kwh_distance")

recent = (
    df_meter_readings
    .filter((F.col("quality") == "EST") & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 87.1).alias("kwh_distance"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isin("EV", "FLAT"))
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.kwh_distance",
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.row_number().over(w_site_tariff_site_id_by_kwh_d).alias("rn"),
        F.max("kwh_distance").over(w_site_id).alias("site_id_max_kwh_distance"),
        F.last("kwh").over(w_site_tariff_site_id_by_kwh_d_2).alias("last_kwh"),
        F.lead("kwh_distance").over(w_site_id_site_tariff_by_kwh_d).alias("next_kwh_distance"),
        F.round(F.col("kwh_distance") / F.nullif(F.sum("kwh_distance").over(w_site_id), F.lit(0)), 4).alias("kwh_distance_share_of_site_id"),
        F.min("kwh_distance").over(w_site_id).alias("site_id_min_kwh_distance"),
        F.dense_rank().over(w_site_id_site_tariff_by_kwh_d_2).alias("kwh_distance_dense_rank"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.when(F.col("site_tariff").isin("FLAT", "TOU"), "group_a").when(F.col("site_tariff") == "EV", "group_b").otherwise("other").alias("site_tariff_group"),
        F.when(F.col("site_tariff").isin("FLAT", "TOU"), "group_a").when(F.col("site_tariff") == "EV", "group_b").otherwise("other").alias("site_tariff_group_2"),
        F.lower(F.trim(F.col("site_tariff"))).alias("site_tariff_clean"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.count(F.lit(1)).over(w_rn_site_tariff_group).alias("rows_in_rn_site_tariff_group"),
        F.first("kwh_distance_dense_rank").over(w_all_by_next_kwh_distance_ful).alias("first_kwh_distance_dense_rank"),
        F.sum("last_kwh").over(w_rn_site_tariff_group).alias("rn_site_tariff_group_sum_last_kwh"),
        (F.col("kwh_distance") - F.lag("kwh_distance").over(w_all_by_next_kwh_distance)).alias("kwh_distance_change"),
        F.max("kwh_distance").over(w_all_by_next_kwh_distance_run).alias("max_kwh_distance_so_far"),
        F.round(F.cume_dist().over(w_rn_site_id_by_next_kwh_dista), 3).alias("next_kwh_distance_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "kwh",
        "kwh_distance",
        "site_tariff",
        "site_id_max_kwh_distance",
        "last_kwh",
        "next_kwh_distance",
        "kwh_distance_share_of_site_id",
        "site_id_min_kwh_distance",
        "kwh_distance_dense_rank",
        "site_tariff_group",
        "site_tariff_group_2",
        "site_tariff_clean",
        "rows_in_rn_site_tariff_group",
        "first_kwh_distance_dense_rank",
        "rn_site_tariff_group_sum_last_kwh",
        "max_kwh_distance_so_far",
        F.round(F.col("kwh_distance_dense_rank") / F.nullif(F.col("max_kwh_distance_so_far"), F.lit(0)), 3).alias("kwh_distance_dense_rank_per_max_kwh_distance_so_"),
        F.lower(F.regexp_replace(F.trim(F.col("site_tariff_group")), " ", "_")).alias("site_tariff_group_slug"),
    )
    .orderBy(F.col("site_tariff_group_2").asc_nulls_last())
)
