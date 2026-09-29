from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost_adj_running = Window.orderBy(F.col("unit_cost_adj").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_is_active_by_unit_c = Window.partitionBy("category", "is_active").orderBy("unit_cost_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_active_category_by_weight = Window.partitionBy("is_active", "category").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_by_unit_cost_adj_ti = Window.partitionBy("category").orderBy(F.col("unit_cost_adj").desc_nulls_first())
w_category_is_active_by_unit_c_2 = Window.partitionBy("category", "is_active").orderBy("unit_cost_adj", "id")
w_all_by_unit_cost = Window.orderBy("unit_cost", "id")
w_all_by_unit_cost_adj = Window.orderBy(F.col("unit_cost_adj").desc_nulls_first(), "id")
w_is_active_category_by_weight_2 = Window.partitionBy("is_active", "category").orderBy("weight_kg", "id")

filtered_skus = (
    df_skus
    .filter((F.col("category") == "Apparel") & (F.col("unit_cost") >= 60.9))
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        F.round(F.col("unit_cost") * 1.07, 2).alias("unit_cost_adj"),
    )
)

metrics = (
    filtered_skus
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "is_active",
        "unit_cost_adj",
        F.last("unit_cost", ignorenulls=True).over(w_all_by_unit_cost_adj_running).alias("last_known_unit_cost"),
        F.sum("unit_cost_adj").over(w_category_is_active_by_unit_c).alias("running_unit_cost_adj"),
        F.last("unit_cost", ignorenulls=True).over(w_is_active_category_by_weight).alias("last_known_unit_cost_2"),
        F.dense_rank().over(w_category_by_unit_cost_adj_ti).alias("unit_cost_adj_dense_rank"),
        F.first("unit_cost", ignorenulls=True).over(w_is_active_category_by_weight).alias("first_known_unit_cost"),
        F.lead("weight_kg").over(w_category_is_active_by_unit_c_2).alias("next_weight_kg"),
        F.ntile(5).over(w_all_by_unit_cost).alias("unit_cost_quintile"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_all_by_unit_cost_adj)).alias("unit_cost_change"),
        F.lead("weight_kg", 3).over(w_category_is_active_by_unit_c_2).alias("next3_weight_kg"),
        (F.col("unit_cost_adj") - F.lag("unit_cost_adj").over(w_is_active_category_by_weight_2)).alias("unit_cost_adj_change"),
    )
)

leaders = (
    metrics
    .filter((F.col("unit_cost_adj_dense_rank") == 1) & (F.col("next3_weight_kg").between(3.7, 20.8)))
    .drop("unit_cost_adj_dense_rank")
)

result = (
    leaders
    .filter(F.col("next_weight_kg") <= 15.2)
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "unit_cost_adj",
        "last_known_unit_cost",
        "running_unit_cost_adj",
        "last_known_unit_cost_2",
        "first_known_unit_cost",
        "next_weight_kg",
        "unit_cost_quintile",
        "unit_cost_change",
        "next3_weight_kg",
        "unit_cost_adj_change",
        (F.floor(F.col("unit_cost_adj") / 100) * 100).alias("unit_cost_adj_bucket"),
    )
)
