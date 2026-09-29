from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost = Window.orderBy("unit_cost", "id")
w_all_by_unit_cost_ties = Window.orderBy("unit_cost")
w_is_active = Window.partitionBy("is_active")

filtered = (
    df_skus
    .filter((F.col("unit_cost") < 251.2) & (~F.col("is_active")))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.coalesce(F.col("unit_cost"), F.lit(0.0)).alias("unit_cost_filled"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.rank().over(w_all_by_unit_cost).alias("unit_cost_rank"),
        F.dense_rank().over(w_all_by_unit_cost_ties).alias("unit_cost_dense_rank"),
        F.sum("unit_cost_filled").over(w_is_active).alias("is_active_sum_unit_cost_filled"),
    )
)

result = (
    ranked
    .cube("unit_cost_dense_rank", "is_active")
    .agg(
        F.grouping("unit_cost_dense_rank").alias("is_total_unit_cost_dense_rank"),
        F.round(F.avg("is_active_sum_unit_cost_filled"), 2).alias("avg_is_active_sum_unit_cost_filled"),
    )
    .orderBy(
        F.col("unit_cost_dense_rank").asc_nulls_last(),
        F.col("is_active").asc_nulls_last(),
    )
)
