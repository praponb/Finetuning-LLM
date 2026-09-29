from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active = Window.partitionBy("is_active")
w_is_active_by_unit_cost_per_w = Window.partitionBy("is_active").orderBy(F.col("unit_cost_per_weight_kg").asc_nulls_last())
w_all_by_unit_cost_per_weight = Window.orderBy(F.col("unit_cost_per_weight_kg").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_unit_cost_per_weight_2 = Window.orderBy(F.col("unit_cost_per_weight_kg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_cost_per_weight_3 = Window.orderBy("unit_cost_per_weight_kg", "id")
w_all_by_unit_cost_per_weight_4 = Window.orderBy("unit_cost_per_weight_kg")
w_all_by_unit_cost_per_weight_5 = Window.orderBy(F.col("unit_cost_per_weight_kg").asc_nulls_last(), "id")
w_all_by_unit_cost_per_weight_6 = Window.orderBy("unit_cost_per_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_active_by_unit_cost_per_w_2 = Window.partitionBy("is_active").orderBy(F.col("unit_cost_per_weight_kg").asc_nulls_last(), "id")
w_unit_cost_per_weight_kg_dens = Window.partitionBy("unit_cost_per_weight_kg_dense_rank", "unit_cost_per_weight_kg_decile").orderBy(F.col("unit_cost_per_weight_kg_change").desc_nulls_first(), "id")
w_unit_cost_per_weight_kg_dens_2 = Window.partitionBy("unit_cost_per_weight_kg_dense_rank", "unit_cost_per_weight_kg_decile").orderBy(F.col("unit_cost_per_weight_kg_change").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_unit_cost_per_weight_kg_dens_3 = Window.partitionBy("unit_cost_per_weight_kg_dense_rank").orderBy(F.col("weight_kg").desc(), "id").rowsBetween(-2, -1)

cleaned_skus = (
    df_skus
    .filter((F.array_contains(F.col("labels"), "hazmat")) & (~F.col("is_active")))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("weight_kg") >= 20.8, "large").when(F.col("weight_kg") >= 12.4, "medium").otherwise("small").alias("weight_kg_tier"),
        F.round(F.col("unit_cost") / F.nullif(F.col("weight_kg"), F.lit(0)), 3).alias("unit_cost_per_weight_kg"),
    )
)

metrics = (
    cleaned_skus
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "labels",
        "weight_kg_tier",
        F.round(F.col("weight_kg") / F.nullif(F.sum("weight_kg").over(w_is_active), F.lit(0)), 4).alias("weight_kg_share_of_is_active"),
        F.dense_rank().over(w_is_active_by_unit_cost_per_w).alias("unit_cost_per_weight_kg_dense_rank"),
        F.round(F.avg("unit_cost_per_weight_kg").over(w_all_by_unit_cost_per_weight), 2).alias("unit_cost_per_weight_kg_avg_4"),
        F.last("weight_kg", ignorenulls=True).over(w_all_by_unit_cost_per_weight_2).alias("last_known_weight_kg"),
        F.lead("unit_cost", 1, 0).over(w_all_by_unit_cost_per_weight_3).alias("next_unit_cost"),
        F.count(F.lit(1)).over(w_is_active).alias("rows_in_is_active"),
        F.round(F.percent_rank().over(w_all_by_unit_cost_per_weight_4), 3).alias("unit_cost_per_weight_kg_percent_rank"),
        (F.col("unit_cost_per_weight_kg") - F.lag("unit_cost_per_weight_kg").over(w_all_by_unit_cost_per_weight_5)).alias("unit_cost_per_weight_kg_change"),
        F.last("unit_cost").over(w_all_by_unit_cost_per_weight_6).alias("last_unit_cost"),
        F.ntile(10).over(w_is_active_by_unit_cost_per_w_2).alias("unit_cost_per_weight_kg_decile"),
        F.first("weight_kg").over(w_all_by_unit_cost_per_weight_6).alias("first_weight_kg"),
    )
)

top_rows = metrics.filter(F.col("unit_cost_per_weight_kg_dense_rank") <= 3)

with_trends = (
    top_rows
    .select(
        "id",
        "unit_cost",
        "labels",
        "weight_kg_tier",
        "weight_kg_share_of_is_active",
        "last_known_weight_kg",
        "next_unit_cost",
        "rows_in_is_active",
        "unit_cost_per_weight_kg_percent_rank",
        "unit_cost_per_weight_kg_change",
        F.lag("unit_cost", 3).over(w_unit_cost_per_weight_kg_dens).alias("prev3_unit_cost"),
        F.round(F.avg("unit_cost").over(w_unit_cost_per_weight_kg_dens_2), 2).alias("unit_cost_avg_2_prior"),
        F.max("last_unit_cost").over(w_unit_cost_per_weight_kg_dens_3).alias("last_unit_cost_max_2_prior"),
    )
)

result = (
    with_trends
    .filter(F.col("weight_kg_tier") == "large")
    .select(
        "id",
        "unit_cost",
        "labels",
        "weight_kg_share_of_is_active",
        "last_known_weight_kg",
        "next_unit_cost",
        "rows_in_is_active",
        "unit_cost_per_weight_kg_percent_rank",
        "unit_cost_per_weight_kg_change",
        "unit_cost_avg_2_prior",
        F.greatest(F.col("last_known_weight_kg"), F.col("rows_in_is_active")).alias("greatest_last_known_weight_kg_rows_in_is_active"),
        F.length(F.trim(F.col("weight_kg_tier"))).alias("weight_kg_tier_len"),
    )
    .orderBy(
        F.col("unit_cost_per_weight_kg_percent_rank").desc(),
        F.col("unit_cost_per_weight_kg_change").desc(),
        "id",
    )
    .limit(20)
)
