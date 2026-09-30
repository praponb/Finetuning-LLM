from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_category_by_unit_c = Window.partitionBy("is_active", "category").orderBy("unit_cost", "id")
w_category_by_weight_kg_adj_ru = Window.partitionBy("category").orderBy("weight_kg_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_is_active_by_weight = Window.partitionBy("category", "is_active").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_active_category = Window.partitionBy("is_active", "category")
w_category_by_weight_kg_adj_fu = Window.partitionBy("category").orderBy("weight_kg_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_is_active_by_weight_2 = Window.partitionBy("category", "is_active").orderBy("weight_kg")
w_category = Window.partitionBy("category")
w_all_by_unit_cost_ties = Window.orderBy(F.col("unit_cost").asc_nulls_last())
w_all_by_unit_cost_full = Window.orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_active_category_by_weight = Window.partitionBy("is_active", "category").orderBy("weight_kg_adj", "id").rowsBetween(-1, -1)
w_is_active_category_by_weight_2 = Window.partitionBy("is_active", "category").orderBy("weight_kg_adj", "id").rowsBetween(-6, -1)

scoped = (
    df_skus
    .filter(F.col("unit_cost") >= 108.5)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.round(F.col("weight_kg") * 0.9, 2).alias("weight_kg_adj"),
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.row_number().over(w_is_active_category_by_unit_c).alias("seq_no"),
        F.last("weight_kg", ignorenulls=True).over(w_category_by_weight_kg_adj_ru).alias("last_known_weight_kg"),
        F.last("unit_cost", ignorenulls=True).over(w_category_is_active_by_weight).alias("last_known_unit_cost"),
        F.round(F.col("weight_kg") - F.avg("weight_kg").over(w_is_active_category), 2).alias("weight_kg_vs_is_active_category_avg"),
        F.nth_value("unit_cost", 2).over(w_category_by_weight_kg_adj_fu).alias("second_unit_cost"),
        F.rank().over(w_category_is_active_by_weight_2).alias("weight_kg_rank"),
        F.dense_rank().over(w_all_by_unit_cost_ties).alias("unit_cost_dense_rank"),
        F.nth_value("unit_cost", 2).over(w_all_by_unit_cost_full).alias("second_unit_cost_2"),
        F.max("unit_cost").over(w_is_active_category_by_weight).alias("unit_cost_max_1_prior"),
        F.max("weight_kg").over(w_category).alias("category_max_weight_kg"),
        F.round(F.avg("unit_cost").over(w_is_active_category_by_weight_2), 2).alias("unit_cost_avg_6_prior"),
    )
)

first_rows = windowed.filter((F.col("seq_no") <= 5) & (F.col("unit_cost") >= 68.0))

result = (
    first_rows
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "weight_kg_adj",
        "seq_no",
        "last_known_weight_kg",
        "last_known_unit_cost",
        "weight_kg_vs_is_active_category_avg",
        "weight_kg_rank",
        "unit_cost_dense_rank",
        "second_unit_cost_2",
        "category_max_weight_kg",
    )
    .orderBy(F.col("weight_kg_adj").desc(), F.col("last_known_unit_cost").desc())
)
