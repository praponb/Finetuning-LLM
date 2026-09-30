from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost_ties = Window.orderBy(F.col("unit_cost").desc())
w_all_by_unit_cost = Window.orderBy(F.col("unit_cost").desc(), "id")
w_weight_kg_tier = Window.partitionBy("weight_kg_tier")
w_all_by_unit_cost_running = Window.orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_weight_kg_tier_by_unit_cost = Window.partitionBy("weight_kg_tier").orderBy("unit_cost_change", "id")

cleaned = (
    df_skus
    .filter((F.col("unit_cost") > 38.3) & (F.array_contains(F.col("labels"), "hazmat")))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "labels",
        F.when(F.col("weight_kg") >= 14.6, "A").when(F.col("weight_kg") >= 12.4, "B").otherwise("C").alias("weight_kg_tier"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_unit_cost_ties), 3).alias("unit_cost_percent_rank"),
        F.lag("unit_cost").over(w_all_by_unit_cost).alias("prev_unit_cost"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_weight_kg_tier), F.lit(0)), 4).alias("unit_cost_share_of_weight_kg_tier"),
        F.count(F.lit(1)).over(w_weight_kg_tier).alias("rows_in_weight_kg_tier"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_all_by_unit_cost)).alias("unit_cost_change"),
        F.count(F.lit(1)).over(w_all_by_unit_cost_running).alias("running_count"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.lead("prev_unit_cost", 3).over(w_weight_kg_tier_by_unit_cost).alias("next3_prev_unit_cost"),
        F.count(F.lit(1)).over(w_weight_kg_tier).alias("rows_in_weight_kg_tier_2"),
        F.sum("prev_unit_cost").over(w_weight_kg_tier).alias("weight_kg_tier_sum_prev_unit_cost"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "unit_cost",
        "labels",
        "weight_kg_tier",
        "unit_cost_percent_rank",
        "prev_unit_cost",
        "rows_in_weight_kg_tier",
        "running_count",
        "next3_prev_unit_cost",
        "rows_in_weight_kg_tier_2",
        "weight_kg_tier_sum_prev_unit_cost",
    )
    .orderBy(F.col("id").desc(), F.col("weight_kg_tier").desc())
)
