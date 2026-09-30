from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_unit_cost_tier_category_by_l = Window.partitionBy("unit_cost_tier", "category").orderBy("least_weight_kg_unit_cost", "id")
w_category = Window.partitionBy("category")
w_unit_cost_tier_category = Window.partitionBy("unit_cost_tier", "category")
w_all_by_least_weight_kg_unit = Window.orderBy(F.col("least_weight_kg_unit_cost").desc(), "id").rowsBetween(-2, Window.currentRow)
w_unit_cost_tier = Window.partitionBy("unit_cost_tier")
w_category_unit_cost_tier_by_u = Window.partitionBy("category", "unit_cost_tier").orderBy("unit_cost", "id").rowsBetween(-6, -1)
w_unit_cost_tier_category_by_l_2 = Window.partitionBy("unit_cost_tier", "category").orderBy(F.col("least_weight_kg_unit_cost").desc(), "id")
w_least_weight_kg_unit_cost_ra = Window.partitionBy("least_weight_kg_unit_cost_rank", "least_weight_kg_unit_cost_dense_rank").orderBy(F.col("least_weight_kg_unit_cost_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_least_weight_kg_unit_cost_ra_2 = Window.partitionBy("least_weight_kg_unit_cost_rank", "least_weight_kg_unit_cost_dense_rank").orderBy(F.col("least_weight_kg_unit_cost_dense_rank").desc(), "id").rowsBetween(-3, -1)
w_least_weight_kg_unit_cost_de = Window.partitionBy("least_weight_kg_unit_cost_dense_rank")
w_least_weight_kg_unit_cost_de_2 = Window.partitionBy("least_weight_kg_unit_cost_dense_rank", "least_weight_kg_unit_cost_rank").orderBy(F.col("rows_in_unit_cost_tier").desc(), "id")

recent_skus = (
    df_skus
    .filter(F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        F.when(F.col("unit_cost") >= 251.2, "A").when(F.col("unit_cost") >= 68.0, "B").otherwise("C").alias("unit_cost_tier"),
        F.least(F.col("weight_kg"), F.col("unit_cost")).alias("least_weight_kg_unit_cost"),
    )
)

sequenced = (
    recent_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "unit_cost_tier",
        "least_weight_kg_unit_cost",
        F.dense_rank().over(w_unit_cost_tier_category_by_l).alias("least_weight_kg_unit_cost_dense_rank"),
        F.max("unit_cost").over(w_category).alias("category_max_unit_cost"),
        F.sum("unit_cost").over(w_unit_cost_tier_category).alias("unit_cost_tier_category_sum_unit_cost"),
        F.max("least_weight_kg_unit_cost").over(w_all_by_least_weight_kg_unit).alias("least_weight_kg_unit_cost_max_3"),
        F.count(F.lit(1)).over(w_unit_cost_tier).alias("rows_in_unit_cost_tier"),
        F.round(F.avg("unit_cost").over(w_category_unit_cost_tier_by_u), 2).alias("unit_cost_avg_6_prior"),
        F.min("least_weight_kg_unit_cost").over(w_unit_cost_tier).alias("unit_cost_tier_min_least_weight_kg_unit_cost"),
        F.lag("least_weight_kg_unit_cost", 3).over(w_unit_cost_tier_category_by_l).alias("prev3_least_weight_kg_unit_cost"),
        F.rank().over(w_unit_cost_tier_category_by_l_2).alias("least_weight_kg_unit_cost_rank"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.first("least_weight_kg_unit_cost").over(w_least_weight_kg_unit_cost_ra).alias("first_least_weight_kg_unit_cost"),
        F.round(F.avg("unit_cost").over(w_least_weight_kg_unit_cost_ra_2), 2).alias("unit_cost_avg_3_prior"),
        F.round(F.col("category_max_unit_cost") / F.nullif(F.sum("category_max_unit_cost").over(w_least_weight_kg_unit_cost_de), F.lit(0)), 4).alias("category_max_unit_cost_share_of_least_weight_kg_"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_least_weight_kg_unit_cost_de), F.lit(0)), 4).alias("unit_cost_share_of_least_weight_kg_unit_cost_den"),
        F.ntile(10).over(w_least_weight_kg_unit_cost_de_2).alias("rows_in_unit_cost_tier_decile"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "least_weight_kg_unit_cost",
        "least_weight_kg_unit_cost_dense_rank",
        "unit_cost_tier_category_sum_unit_cost",
        "unit_cost_avg_6_prior",
        "unit_cost_tier_min_least_weight_kg_unit_cost",
        "prev3_least_weight_kg_unit_cost",
        "least_weight_kg_unit_cost_rank",
        "first_least_weight_kg_unit_cost",
        "unit_cost_share_of_least_weight_kg_unit_cost_den",
    )
)
