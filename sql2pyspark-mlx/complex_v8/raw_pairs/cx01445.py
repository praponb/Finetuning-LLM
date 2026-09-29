from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category = Window.partitionBy("category")
w_category_by_weight_kg_prior = Window.partitionBy("category").orderBy("weight_kg", "id").rowsBetween(-3, -1)
w_category_by_weight_kg_full = Window.partitionBy("category").orderBy(F.col("weight_kg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_weight_kg_full = Window.orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_by_weight_kg_prior_2 = Window.partitionBy("category").orderBy(F.col("weight_kg").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_category_by_weight_kg_runnin = Window.partitionBy("category").orderBy(F.col("weight_kg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_category_max_unit_cos = Window.orderBy(F.col("category_max_unit_cost").asc_nulls_last(), "id")
w_all_by_unit_cost_share_of_ca = Window.orderBy(F.col("unit_cost_share_of_category").desc(), "id").rowsBetween(-3, -1)
w_all_by_unit_cost_share_of_ca_2 = Window.orderBy(F.col("unit_cost_share_of_category").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_cost_share_of_ca_3 = Window.orderBy(F.col("unit_cost_share_of_category").desc(), "id")
w_all_by_unit_cost_share_of_ca_4 = Window.orderBy(F.col("unit_cost_share_of_category").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_skus
    .filter((F.col("category") == "Apparel") & (F.upper(F.trim(F.col("sku_code"))).like("A%")))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.substring(F.trim(F.col("sku_code")), 1, 2).alias("sku_code_prefix"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "sku_code_prefix",
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_category), F.lit(0)), 4).alias("unit_cost_share_of_category"),
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
        F.round(F.avg("weight_kg").over(w_category_by_weight_kg_prior), 2).alias("weight_kg_avg_3_prior"),
        F.last("unit_cost").over(w_category_by_weight_kg_full).alias("last_unit_cost"),
        F.last("unit_cost").over(w_all_by_weight_kg_full).alias("last_unit_cost_2"),
        F.sum("unit_cost").over(w_category_by_weight_kg_prior_2).alias("unit_cost_sum_2_prior"),
        F.max("unit_cost").over(w_category).alias("category_max_unit_cost"),
        F.last("weight_kg", ignorenulls=True).over(w_category_by_weight_kg_runnin).alias("last_known_weight_kg"),
    )
)

derived = (
    with_windows
    .filter(F.col("category_max_unit_cost") < 2800.3)
    .withColumn("sku_code_prefix_clean", F.initcap(F.trim(F.col("sku_code_prefix"))))
)

with_trends = (
    derived
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "sku_code_prefix",
        "unit_cost_share_of_category",
        "unit_cost_sum_2_prior",
        F.rank().over(w_all_by_category_max_unit_cos).alias("category_max_unit_cost_rank"),
        F.max("unit_cost").over(w_all_by_unit_cost_share_of_ca).alias("unit_cost_max_3_prior"),
        F.ntile(4).over(w_all_by_category_max_unit_cos).alias("category_max_unit_cost_quartile"),
        F.first("last_unit_cost", ignorenulls=True).over(w_all_by_unit_cost_share_of_ca_2).alias("first_known_last_unit_cost"),
        F.row_number().over(w_all_by_unit_cost_share_of_ca_3).alias("seq_no"),
        F.lead("unit_cost_sum_2_prior", 1, 0).over(w_all_by_unit_cost_share_of_ca_3).alias("next_unit_cost_sum_2_prior"),
        F.last("weight_kg_avg_3_prior").over(w_all_by_unit_cost_share_of_ca_4).alias("last_weight_kg_avg_3_prior"),
    )
)

result = (
    with_trends
    .filter(F.col("category_max_unit_cost_quartile") <= 8)
    .select(
        "id",
        "unit_cost",
        "unit_cost_share_of_category",
        "unit_cost_sum_2_prior",
        "category_max_unit_cost_rank",
        "unit_cost_max_3_prior",
        "category_max_unit_cost_quartile",
        "first_known_last_unit_cost",
        "seq_no",
        F.round(F.col("unit_cost_max_3_prior") / F.nullif(F.col("seq_no"), F.lit(0)), 3).alias("unit_cost_max_3_prior_per_seq_no"),
        F.round(F.col("unit_cost") / F.nullif(F.col("seq_no"), F.lit(0)), 3).alias("unit_cost_per_seq_no"),
    )
    .orderBy(F.col("category_max_unit_cost_quartile").desc_nulls_first(), "id")
    .limit(10)
)
