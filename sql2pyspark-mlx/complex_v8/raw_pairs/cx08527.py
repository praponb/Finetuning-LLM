from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_category_by_unit_c = Window.partitionBy("is_active", "category").orderBy("unit_cost", "id").rowsBetween(-3, Window.currentRow)
w_category_is_active_by_weight = Window.partitionBy("category", "is_active").orderBy("weight_kg")
w_category_is_active_by_weight_2 = Window.partitionBy("category", "is_active").orderBy("weight_kg", "id")
w_category_is_active_by_weight_3 = Window.partitionBy("category", "is_active").orderBy("weight_kg", "id").rowsBetween(-3, Window.currentRow)
w_is_active_by_unit_cost = Window.partitionBy("is_active").orderBy("unit_cost", "id")
w_is_active_category = Window.partitionBy("is_active", "category")
w_is_active_by_unit_cost_ties = Window.partitionBy("is_active").orderBy("unit_cost")
w_category_is_active_by_weight_4 = Window.partitionBy("category", "is_active").orderBy("weight_kg", "id").rowsBetween(-3, -1)
w_unit_cost_dense_rank_by_prev = Window.partitionBy("unit_cost_dense_rank").orderBy("prev_unit_cost_bucket", "id")
w_unit_cost_dense_rank_by_prev_2 = Window.partitionBy("unit_cost_dense_rank").orderBy("prev_unit_cost_bucket", "id").rowsBetween(-3, -1)
w_unit_cost_dense_rank_by_unit = Window.partitionBy("unit_cost_dense_rank").orderBy(F.col("unit_cost_bucket_sum_4").desc())

valid_skus = (
    df_skus
    .filter(~F.array_contains(F.col("labels"), "fragile"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        (F.floor(F.col("unit_cost") / 50) * 50).alias("unit_cost_bucket"),
        F.concat(F.col("category"), F.lit("#"), F.col("id").cast("string")).alias("category_ref"),
    )
)

sequenced = (
    valid_skus
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.round(F.avg("unit_cost").over(w_is_active_category_by_unit_c), 2).alias("unit_cost_avg_4"),
        F.round(F.cume_dist().over(w_category_is_active_by_weight), 3).alias("weight_kg_cume_dist"),
        F.ntile(4).over(w_category_is_active_by_weight_2).alias("weight_kg_quartile"),
        F.sum("unit_cost_bucket").over(w_category_is_active_by_weight_3).alias("unit_cost_bucket_sum_4"),
        F.lead("weight_kg", 3).over(w_is_active_by_unit_cost).alias("next3_weight_kg"),
        F.lag("unit_cost_bucket").over(w_is_active_by_unit_cost).alias("prev_unit_cost_bucket"),
        F.rank().over(w_is_active_by_unit_cost_ties).alias("unit_cost_rank"),
        F.dense_rank().over(w_is_active_by_unit_cost_ties).alias("unit_cost_dense_rank"),
        F.max("unit_cost").over(w_category_is_active_by_weight_4).alias("unit_cost_max_3_prior"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.lead("prev_unit_cost_bucket").over(w_unit_cost_dense_rank_by_prev).alias("next_prev_unit_cost_bucket"),
        F.round(F.avg("unit_cost_max_3_prior").over(w_unit_cost_dense_rank_by_prev_2), 2).alias("unit_cost_max_3_prior_avg_3_prior"),
        F.round(F.cume_dist().over(w_unit_cost_dense_rank_by_unit), 3).alias("unit_cost_bucket_sum_4_cume_dist"),
        F.lag("unit_cost_rank").over(w_unit_cost_dense_rank_by_prev).alias("prev_unit_cost_rank"),
    )
)

result = (
    metrics
    .select("next_prev_unit_cost_bucket")
    .distinct()
    .orderBy(F.col("next_prev_unit_cost_bucket").desc())
)
