from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_is_active_by_unit_c = Window.partitionBy("category", "is_active").orderBy(F.col("unit_cost").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_is_active_by_weight = Window.partitionBy("category", "is_active").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_weight_kg_running = Window.orderBy(F.col("weight_kg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_weight_kg_ties = Window.orderBy(F.col("weight_kg").desc_nulls_first())
w_all_by_weight_kg = Window.orderBy(F.col("weight_kg").desc_nulls_first(), "id")
w_category_is_active_by_unit_c_2 = Window.partitionBy("category", "is_active").orderBy(F.col("unit_cost").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_category_is_active_by_weight_2 = Window.partitionBy("category", "is_active").orderBy("weight_kg", "id")

cleaned = (
    df_skus
    .filter(F.array_contains(F.col("labels"), "hazmat"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.nth_value("weight_kg", 2).over(w_category_is_active_by_unit_c).alias("second_weight_kg"),
        F.last("unit_cost").over(w_category_is_active_by_unit_c).alias("last_unit_cost"),
        F.count(F.lit(1)).over(w_category_is_active_by_weight).alias("running_count"),
        F.last("weight_kg", ignorenulls=True).over(w_all_by_weight_kg_running).alias("last_known_weight_kg"),
        F.first("unit_cost", ignorenulls=True).over(w_category_is_active_by_weight).alias("first_known_unit_cost"),
        F.round(F.percent_rank().over(w_all_by_weight_kg_ties), 3).alias("weight_kg_percent_rank"),
        F.lead("unit_cost", 2).over(w_all_by_weight_kg).alias("next2_unit_cost"),
        F.round(F.avg("weight_kg").over(w_category_is_active_by_unit_c_2), 2).alias("weight_kg_avg_2_prior"),
        F.rank().over(w_category_is_active_by_weight_2).alias("weight_kg_rank"),
    )
)

derived = (
    metrics
    .filter(F.col("next2_unit_cost").between(108.5, 358.5))
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "labels",
        "running_count",
        "last_known_weight_kg",
        "first_known_unit_cost",
        "weight_kg_percent_rank",
        "weight_kg_avg_2_prior",
        "weight_kg_rank",
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
        F.concat_ws(": ", F.col("category"), F.col("sku_code")).alias("category_sku_code_label"),
    )
)

result = (
    derived
    .filter(F.col("last_known_weight_kg") >= 1.8)
    .select(
        "id",
        "weight_kg",
        "labels",
        "running_count",
        "weight_kg_percent_rank",
        "weight_kg_avg_2_prior",
        "weight_kg_rank",
        "is_active_flag",
        "category_sku_code_label",
    )
    .orderBy("category_sku_code_label")
)
