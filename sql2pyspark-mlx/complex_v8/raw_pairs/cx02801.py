from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_len_by_category_len = Window.partitionBy("category_len").orderBy("category_len")
w_category_by_weight_kg_ties = Window.partitionBy("category").orderBy("weight_kg")
w_category_or_unknown_by_categ = Window.partitionBy("category_or_unknown").orderBy(F.col("category_len").desc(), "id")
w_category_len_by_category_len_2 = Window.partitionBy("category_len").orderBy("category_len", "id")
w_category_category_or_unknown = Window.partitionBy("category", "category_or_unknown")
w_category_or_unknown = Window.partitionBy("category_or_unknown")
w_category_or_unknown_by_categ_2 = Window.partitionBy("category_or_unknown").orderBy(F.col("category_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_or_unknown_by_categ_3 = Window.partitionBy("category_or_unknown").orderBy(F.col("category_len").desc())
w_category_or_unknown_by_categ_4 = Window.partitionBy("category_or_unknown").orderBy("category_len", "id")

base_skus = (
    df_skus
    .filter((F.length(F.trim(F.col("sku_code"))) >= 5) & (~F.array_contains(F.col("labels"), "express")))
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
        F.length(F.trim(F.col("category"))).alias("category_len"),
    )
)

metrics = (
    base_skus
    .select(
        "id",
        "unit_cost",
        "category_len",
        F.round(F.cume_dist().over(w_category_len_by_category_len), 3).alias("category_len_cume_dist"),
        F.dense_rank().over(w_category_by_weight_kg_ties).alias("weight_kg_dense_rank"),
        F.round(F.percent_rank().over(w_category_or_unknown_by_categ), 3).alias("category_len_percent_rank"),
        F.round(F.cume_dist().over(w_category_len_by_category_len_2), 3).alias("category_len_cume_dist_2"),
        F.sum("weight_kg").over(w_category_category_or_unknown).alias("category_category_or_unknown_sum_weight_kg"),
        (F.col("category_len") - F.lag("category_len").over(w_category_len_by_category_len_2)).alias("category_len_change"),
        F.round(F.col("category_len") - F.avg("category_len").over(w_category_or_unknown), 2).alias("category_len_vs_category_or_unknown_avg"),
        F.round(F.avg("unit_cost").over(w_category_category_or_unknown), 2).alias("category_category_or_unknown_avg_unit_cost"),
        F.nth_value("category_len", 2).over(w_category_or_unknown_by_categ_2).alias("second_category_len"),
        F.lead("unit_cost").over(w_category_or_unknown_by_categ).alias("next_unit_cost"),
        F.round(F.avg("weight_kg").over(w_category_category_or_unknown), 2).alias("category_category_or_unknown_avg_weight_kg"),
        F.round(F.cume_dist().over(w_category_or_unknown_by_categ_3), 3).alias("category_len_cume_dist_3"),
        F.lag("unit_cost").over(w_category_or_unknown_by_categ_4).alias("prev_unit_cost"),
    )
)

result = (
    metrics
    .select(
        "id",
        "unit_cost",
        "category_len_cume_dist",
        "weight_kg_dense_rank",
        "category_len_percent_rank",
        "category_len_cume_dist_2",
        "category_category_or_unknown_sum_weight_kg",
        "category_len_change",
        "category_len_vs_category_or_unknown_avg",
        "second_category_len",
        "category_len_cume_dist_3",
        "prev_unit_cost",
        (F.col("category_len_cume_dist_3") + F.col("category_len_cume_dist")).alias("category_len_cume_dist_3_plus_category_len_cume_"),
        (F.col("category_len_cume_dist_3") - F.col("unit_cost")).alias("category_len_cume_dist_3_minus_unit_cost"),
    )
    .orderBy(
        F.col("weight_kg_dense_rank").desc_nulls_first(),
        F.col("category_len_cume_dist_3_plus_category_len_cume_").asc(),
    )
)
