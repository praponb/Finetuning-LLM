from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_is_active = Window.partitionBy("category", "is_active")
w_is_active_by_weight_kg_movin = Window.partitionBy("is_active").orderBy("weight_kg", "id").rowsBetween(-2, Window.currentRow)
w_all_by_unit_cost_running = Window.orderBy(F.col("unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_cost = Window.orderBy(F.col("unit_cost").desc(), "id")
w_is_active_category = Window.partitionBy("is_active", "category")
w_category_by_unit_cost_full = Window.partitionBy("category").orderBy(F.col("unit_cost").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_by_unit_cost_ties = Window.partitionBy("category").orderBy(F.col("unit_cost").desc_nulls_first())
w_is_active_category_by_weight = Window.partitionBy("is_active", "category").orderBy("weight_kg")
w_is_active_by_unit_cost_ties = Window.partitionBy("is_active").orderBy("unit_cost")

base = df_skus.filter(F.lower(F.col("sku_code")).like("%y-%"))

ranked = (
    base
    .select(
        "id",
        "sku_code",
        "category",
        "labels",
        "is_active",
        F.max("unit_cost").over(w_category_is_active).alias("category_is_active_max_unit_cost"),
        F.count(F.lit(1)).over(w_category_is_active).alias("rows_in_category_is_active"),
        F.round(F.avg("unit_cost").over(w_is_active_by_weight_kg_movin), 2).alias("unit_cost_avg_3"),
        F.last("weight_kg", ignorenulls=True).over(w_all_by_unit_cost_running).alias("last_known_weight_kg"),
        F.lag("unit_cost", 3, 0).over(w_all_by_unit_cost).alias("prev3_unit_cost"),
        F.round(F.col("weight_kg") / F.nullif(F.sum("weight_kg").over(w_is_active_category), F.lit(0)), 4).alias("weight_kg_share_of_is_active_category"),
        F.first("unit_cost").over(w_category_by_unit_cost_full).alias("first_unit_cost"),
        F.rank().over(w_category_by_unit_cost_ties).alias("unit_cost_rank"),
        F.rank().over(w_is_active_category_by_weight).alias("weight_kg_rank"),
        F.round(F.percent_rank().over(w_is_active_by_unit_cost_ties), 3).alias("unit_cost_percent_rank"),
        F.sum("unit_cost").over(w_is_active_category).alias("is_active_category_sum_unit_cost"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_code",
        "category",
        "labels",
        "weight_kg_share_of_is_active_category",
        "first_unit_cost",
        "unit_cost_rank",
        "weight_kg_rank",
        "unit_cost_percent_rank",
        "is_active_category_sum_unit_cost",
        F.upper(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)
