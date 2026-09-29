from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_by_weight_kg_bucket = Window.partitionBy("category").orderBy(F.col("weight_kg_bucket").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_category_by_unit_cost = Window.partitionBy("category").orderBy("unit_cost", "id")
w_category_by_unit_cost_2 = Window.partitionBy("category").orderBy(F.col("unit_cost").desc(), "id")
w_category = Window.partitionBy("category")
w_category_by_unit_cost_runnin = Window.partitionBy("category").orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_by_weight_kg_bucket_2 = Window.partitionBy("category").orderBy(F.col("weight_kg_bucket").desc_nulls_first(), "id")
w_category_by_unit_cost_runnin_2 = Window.partitionBy("category").orderBy(F.col("unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_by_unit_cost_full = Window.partitionBy("category").orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_seq_no = Window.partitionBy("category", "seq_no")
w_weight_kg_bucket_rank_catego = Window.partitionBy("weight_kg_bucket_rank", "category").orderBy("weight_kg_bucket_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_weight_kg_bucket_ra = Window.partitionBy("category", "weight_kg_bucket_rank").orderBy(F.col("last_weight_kg_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_skus = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("A%"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        (F.floor(F.col("weight_kg") / 5) * 5).alias("weight_kg_bucket"),
        F.lower(F.trim(F.col("category"))).alias("category_clean"),
    )
)

with_windows = (
    cleaned_skus
    .select(
        "*",
        F.round(F.avg("unit_cost").over(w_category_by_weight_kg_bucket), 2).alias("unit_cost_avg_3_prior"),
        (F.col("weight_kg_bucket") - F.lag("weight_kg_bucket").over(w_category_by_unit_cost)).alias("weight_kg_bucket_change"),
        F.row_number().over(w_category_by_unit_cost_2).alias("seq_no"),
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_category_by_unit_cost_2)).alias("unit_cost_change"),
        F.sum("unit_cost").over(w_category_by_weight_kg_bucket).alias("unit_cost_sum_3_prior"),
        F.last("unit_cost", ignorenulls=True).over(w_category_by_unit_cost_runnin).alias("last_known_unit_cost"),
        F.rank().over(w_category_by_weight_kg_bucket_2).alias("weight_kg_bucket_rank"),
        F.round(F.cume_dist().over(w_category_by_weight_kg_bucket_2), 3).alias("weight_kg_bucket_cume_dist"),
        F.first("weight_kg_bucket", ignorenulls=True).over(w_category_by_unit_cost_runnin_2).alias("first_known_weight_kg_bucket"),
        F.last("weight_kg_bucket").over(w_category_by_unit_cost_full).alias("last_weight_kg_bucket"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "category",
        "unit_cost",
        "labels",
        "weight_kg_bucket",
        "category_clean",
        "weight_kg_bucket_change",
        "seq_no",
        "unit_cost_change",
        "last_known_unit_cost",
        "weight_kg_bucket_rank",
        "weight_kg_bucket_cume_dist",
        "first_known_weight_kg_bucket",
        F.round(F.col("unit_cost_sum_3_prior") - F.avg("unit_cost_sum_3_prior").over(w_category_seq_no), 2).alias("unit_cost_sum_3_prior_vs_category_seq_no_avg"),
        F.first("weight_kg_bucket").over(w_weight_kg_bucket_rank_catego).alias("first_weight_kg_bucket"),
        F.count(F.lit(1)).over(w_category_seq_no).alias("rows_in_category_seq_no"),
        F.round(F.col("first_known_weight_kg_bucket") - F.avg("first_known_weight_kg_bucket").over(w_category_seq_no), 2).alias("first_known_weight_kg_bucket_vs_category_seq_no_"),
        F.nth_value("unit_cost_sum_3_prior", 2).over(w_category_weight_kg_bucket_ra).alias("second_unit_cost_sum_3_prior"),
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_category_seq_no), 2).alias("seq_no_vs_category_seq_no_avg"),
    )
)

result = (
    ranked
    .select(
        "id",
        "unit_cost",
        "labels",
        "category_clean",
        "weight_kg_bucket_change",
        "unit_cost_change",
        "last_known_unit_cost",
        "weight_kg_bucket_rank",
        "weight_kg_bucket_cume_dist",
        "first_known_weight_kg_bucket",
        "unit_cost_sum_3_prior_vs_category_seq_no_avg",
        "first_weight_kg_bucket",
        "first_known_weight_kg_bucket_vs_category_seq_no_",
        "second_unit_cost_sum_3_prior",
        F.length(F.trim(F.col("category"))).alias("category_len"),
    )
    .orderBy(F.col("first_weight_kg_bucket").asc_nulls_last(), "id")
    .limit(20)
)
