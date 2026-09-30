from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_is_active_label_by = Window.partitionBy("is_active", "is_active_label").orderBy(F.col("weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_active_by_weight_kg = Window.partitionBy("is_active").orderBy(F.col("weight_kg").desc(), "id")
w_is_active_category_by_weight = Window.partitionBy("is_active", "category").orderBy("weight_kg", "id")
w_category = Window.partitionBy("category")
w_category_is_active = Window.partitionBy("category", "is_active")
w_category_by_unit_cost_moving = Window.partitionBy("category").orderBy(F.col("unit_cost").desc(), "id").rowsBetween(-3, Window.currentRow)
w_is_active_category_by_weight_2 = Window.partitionBy("is_active", "category").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_by_unit_cost = Window.partitionBy("category").orderBy(F.col("unit_cost").desc(), "id")
w_category_by_unit_cost_moving_2 = Window.partitionBy("category").orderBy(F.col("unit_cost").desc(), "id").rowsBetween(-1, Window.currentRow)

scoped = (
    df_skus
    .filter((F.col("unit_cost") >= 108.5) & (F.size(F.col("labels")) >= 2))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.first("unit_cost").over(w_is_active_is_active_label_by).alias("first_unit_cost"),
        F.row_number().over(w_is_active_by_weight_kg).alias("weight_kg_seq"),
        F.lead("unit_cost", 3, 0).over(w_is_active_category_by_weight).alias("next3_unit_cost"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_category), F.lit(0)), 4).alias("unit_cost_share_of_category"),
        F.round(F.col("unit_cost") - F.avg("unit_cost").over(w_category_is_active), 2).alias("unit_cost_vs_category_is_active_avg"),
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_category_is_active), F.lit(0)), 4).alias("unit_cost_share_of_category_is_active"),
        F.sum("weight_kg").over(w_category_by_unit_cost_moving).alias("weight_kg_sum_4"),
        F.max("weight_kg").over(w_is_active_category_by_weight_2).alias("max_weight_kg_so_far"),
        F.round(F.col("weight_kg") / F.nullif(F.sum("weight_kg").over(w_category_is_active), F.lit(0)), 4).alias("weight_kg_share_of_category_is_active"),
        F.row_number().over(w_category_by_unit_cost).alias("seq_no"),
        F.round(F.percent_rank().over(w_is_active_category_by_weight), 3).alias("weight_kg_percent_rank"),
        F.sum("weight_kg").over(w_category_by_unit_cost_moving_2).alias("weight_kg_sum_2"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.size(F.col("labels")).alias("labels_count"),
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
)

result = (
    labeled
    .select(
        "id",
        "sku_code",
        "category",
        "labels",
        "is_active",
        "is_active_label",
        "first_unit_cost",
        "weight_kg_seq",
        "next3_unit_cost",
        "unit_cost_share_of_category",
        "unit_cost_vs_category_is_active_avg",
        "unit_cost_share_of_category_is_active",
        "weight_kg_sum_4",
        "max_weight_kg_so_far",
        "weight_kg_share_of_category_is_active",
        "seq_no",
        "weight_kg_percent_rank",
        "weight_kg_sum_2",
        "labels_count",
        "sku_code_len",
    )
    .orderBy(F.col("unit_cost_share_of_category_is_active").desc(), "id")
    .limit(5)
)
