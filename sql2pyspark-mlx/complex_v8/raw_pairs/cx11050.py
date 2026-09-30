from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_category = Window.partitionBy("is_active", "category")
w_category = Window.partitionBy("category")
w_category_is_active_by_unit_c = Window.partitionBy("category", "is_active").orderBy("unit_cost", "id")
w_all_by_weight_kg_full = Window.orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_weight_kg_moving_6 = Window.orderBy("weight_kg", "id").rowsBetween(-6, Window.currentRow)
w_is_active_category_by_weight = Window.partitionBy("is_active", "category").orderBy(F.col("weight_kg").desc_nulls_first(), "id")
w_is_active_category_by_weight_2 = Window.partitionBy("is_active", "category").orderBy(F.col("weight_kg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_by_seq_no_ties = Window.partitionBy("category").orderBy("seq_no")
w_category_by_seq_no = Window.partitionBy("category").orderBy("seq_no", "id")
w_category_by_unit_cost_share = Window.partitionBy("category").orderBy(F.col("unit_cost_share_of_category").desc(), "id")
w_seq_no_unit_cost_quartile_by = Window.partitionBy("seq_no", "unit_cost_quartile").orderBy(F.col("unit_cost_quartile").desc())
w_seq_no_by_weight_kg_avg_7_ru = Window.partitionBy("seq_no").orderBy(F.col("weight_kg_avg_7").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_skus = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.substring(F.trim(F.col("sku_code")), 1, 3).alias("sku_code_prefix"),
    )
)

with_trends = (
    cleaned_skus
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_active_category).alias("rows_in_is_active_category"),
        (F.col("weight_kg") - F.lag("weight_kg").over(w_category_is_active_by_unit_c)).alias("weight_kg_change"),
        F.round(F.col("unit_cost") - F.avg("unit_cost").over(w_category), 2).alias("unit_cost_vs_category_avg"),
        F.nth_value("weight_kg", 2).over(w_all_by_weight_kg_full).alias("second_weight_kg"),
        F.round(F.avg("weight_kg").over(w_all_by_weight_kg_moving_6), 2).alias("weight_kg_avg_7"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_category), F.lit(0)), 4).alias("unit_cost_share_of_category"),
        F.ntile(4).over(w_category_is_active_by_unit_c).alias("unit_cost_quartile"),
        F.row_number().over(w_is_active_category_by_weight).alias("seq_no"),
        F.last("unit_cost").over(w_is_active_category_by_weight_2).alias("last_unit_cost"),
        F.min("unit_cost").over(w_category).alias("category_min_unit_cost"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.dense_rank().over(w_category_by_seq_no_ties).alias("seq_no_dense_rank"),
        F.lag("weight_kg_avg_7").over(w_category_by_seq_no).alias("prev_weight_kg_avg_7"),
        F.row_number().over(w_category_by_unit_cost_share).alias("rn"),
        F.rank().over(w_category_by_seq_no).alias("seq_no_rank"),
        F.round(F.percent_rank().over(w_category_by_seq_no_ties), 3).alias("seq_no_percent_rank"),
        F.rank().over(w_seq_no_unit_cost_quartile_by).alias("unit_cost_quartile_rank"),
        F.sum("unit_cost_share_of_category").over(w_seq_no_by_weight_kg_avg_7_ru).alias("running_unit_cost_share_of_category"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        "rows_in_is_active_category",
        "weight_kg_change",
        "unit_cost_vs_category_avg",
        "second_weight_kg",
        "weight_kg_avg_7",
        "unit_cost_share_of_category",
        "unit_cost_quartile",
        "seq_no",
        "last_unit_cost",
        "category_min_unit_cost",
        "seq_no_dense_rank",
        "prev_weight_kg_avg_7",
        "seq_no_rank",
        "seq_no_percent_rank",
        "unit_cost_quartile_rank",
    )
    .orderBy(F.col("prev_weight_kg_avg_7").desc_nulls_first())
)
