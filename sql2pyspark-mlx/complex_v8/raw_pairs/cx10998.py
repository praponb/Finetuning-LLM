from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost_full = Window.orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_by_weight_kg_runnin = Window.partitionBy("category").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_active_by_weight_kg_ties = Window.partitionBy("is_active").orderBy(F.col("weight_kg").desc())
w_is_active_category_by_unit_c = Window.partitionBy("is_active", "category").orderBy("unit_cost", "id")
w_is_active = Window.partitionBy("is_active")
w_all_by_unit_cost = Window.orderBy("unit_cost", "id")
w_category_by_weight_kg = Window.partitionBy("category").orderBy("weight_kg", "id")
w_is_active_category_by_unit_c_2 = Window.partitionBy("is_active", "category").orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_cost_running = Window.orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_cost_ties = Window.orderBy("unit_cost")

valid_skus = (
    df_skus
    .filter(F.col("category").isin("Food", "Electronics"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
    )
)

sequenced = (
    valid_skus
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        F.last("weight_kg").over(w_all_by_unit_cost_full).alias("last_weight_kg"),
        F.first("weight_kg", ignorenulls=True).over(w_category_by_weight_kg_runnin).alias("first_known_weight_kg"),
        F.dense_rank().over(w_is_active_by_weight_kg_ties).alias("weight_kg_dense_rank"),
        F.lag("unit_cost").over(w_is_active_category_by_unit_c).alias("prev_unit_cost"),
        F.count(F.lit(1)).over(w_is_active).alias("rows_in_is_active"),
        F.rank().over(w_all_by_unit_cost).alias("unit_cost_rank"),
        F.round(F.col("weight_kg") - F.avg("weight_kg").over(w_is_active), 2).alias("weight_kg_vs_is_active_avg"),
        F.round(F.cume_dist().over(w_category_by_weight_kg), 3).alias("weight_kg_cume_dist"),
        F.first("weight_kg", ignorenulls=True).over(w_is_active_category_by_unit_c_2).alias("first_known_weight_kg_2"),
        F.min("unit_cost").over(w_all_by_unit_cost_running).alias("min_unit_cost_so_far"),
        F.max("unit_cost").over(w_is_active).alias("is_active_max_unit_cost"),
        F.lead("unit_cost", 3).over(w_is_active_category_by_unit_c).alias("next3_unit_cost"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_is_active), F.lit(0)), 4).alias("unit_cost_share_of_is_active"),
        F.round(F.cume_dist().over(w_all_by_unit_cost_ties), 3).alias("unit_cost_cume_dist"),
    )
)

top_per_group = (
    sequenced
    .filter(F.col("weight_kg_dense_rank") <= 5)
    .drop("weight_kg_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "is_active",
        "last_weight_kg",
        "rows_in_is_active",
        "weight_kg_vs_is_active_avg",
        "weight_kg_cume_dist",
        "first_known_weight_kg_2",
        "min_unit_cost_so_far",
        "is_active_max_unit_cost",
        "next3_unit_cost",
        "unit_cost_share_of_is_active",
        "unit_cost_cume_dist",
    )
    .orderBy(
        F.col("weight_kg_cume_dist").asc(),
        F.col("rows_in_is_active").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
