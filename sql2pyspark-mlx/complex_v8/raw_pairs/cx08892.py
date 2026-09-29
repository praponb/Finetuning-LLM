from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_by_unit_cost = Window.partitionBy("is_active").orderBy(F.col("unit_cost").desc(), "id")
w_all_by_unit_cost_bucket = Window.orderBy(F.col("unit_cost_bucket").asc_nulls_last(), "id")
w_is_active_category_by_unit_c = Window.partitionBy("is_active", "category").orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_by_unit_cost_bucket = Window.partitionBy("category").orderBy("unit_cost_bucket", "id")
w_is_active_category = Window.partitionBy("is_active", "category")
w_is_active = Window.partitionBy("is_active")
w_category_is_active = Window.partitionBy("category", "is_active")
w_seq_no_3 = Window.partitionBy("seq_no_3")
w_seq_no_by_unit_cost_bucket = Window.partitionBy("seq_no").orderBy("unit_cost_bucket", "id")
w_seq_no_3_category_by_unit_co = Window.partitionBy("seq_no_3", "category").orderBy(F.col("unit_cost").desc())

valid_skus = (
    df_skus
    .filter((F.col("weight_kg") >= 6.4) & (F.col("category").isin("Toys", "Apparel", "Electronics")))
    .select(
        "id",
        "category",
        "unit_cost",
        "labels",
        "is_active",
        F.lower(F.regexp_replace(F.trim(F.col("sku_code")), " ", "_")).alias("sku_code_slug"),
        (F.floor(F.col("unit_cost") / 50) * 50).alias("unit_cost_bucket"),
    )
)

with_windows = (
    valid_skus
    .select(
        "*",
        F.row_number().over(w_is_active_by_unit_cost).alias("seq_no"),
        F.row_number().over(w_all_by_unit_cost_bucket).alias("seq_no_2"),
        F.first("unit_cost").over(w_is_active_category_by_unit_c).alias("first_unit_cost"),
        F.row_number().over(w_category_by_unit_cost_bucket).alias("seq_no_3"),
        F.round(F.col("unit_cost_bucket") - F.avg("unit_cost_bucket").over(w_is_active_category), 2).alias("unit_cost_bucket_vs_is_active_category_avg"),
        F.sum("unit_cost").over(w_is_active).alias("is_active_sum_unit_cost"),
        F.max("unit_cost").over(w_category_is_active).alias("category_is_active_max_unit_cost"),
        F.count(F.lit(1)).over(w_category_is_active).alias("rows_in_category_is_active"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.round(F.col("seq_no_2") / F.nullif(F.sum("seq_no_2").over(w_seq_no_3), F.lit(0)), 4).alias("seq_no_2_share_of_seq_no_3"),
        F.lead("rows_in_category_is_active").over(w_seq_no_by_unit_cost_bucket).alias("next_rows_in_category_is_active"),
        F.count(F.lit(1)).over(w_seq_no_3).alias("rows_in_seq_no_3"),
        F.rank().over(w_seq_no_3_category_by_unit_co).alias("unit_cost_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "category",
        "unit_cost",
        "labels",
        "is_active",
        "sku_code_slug",
        "seq_no",
        "seq_no_2",
        "first_unit_cost",
        "seq_no_3",
        "unit_cost_bucket_vs_is_active_category_avg",
        "is_active_sum_unit_cost",
        "category_is_active_max_unit_cost",
        "rows_in_category_is_active",
        "seq_no_2_share_of_seq_no_3",
        "rows_in_seq_no_3",
        "unit_cost_rank",
        F.lower(F.regexp_replace(F.trim(F.col("sku_code_slug")), " ", "_")).alias("sku_code_slug_slug"),
        (F.floor(F.col("rows_in_seq_no_3") / 5) * 5).alias("rows_in_seq_no_3_bucket"),
    )
    .orderBy(
        "rows_in_seq_no_3_bucket",
        F.col("category_is_active_max_unit_cost").desc_nulls_first(),
    )
)
