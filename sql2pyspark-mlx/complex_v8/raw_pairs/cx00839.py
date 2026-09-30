from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_code_len = Window.partitionBy("sku_code_len")
w_all_by_unit_cost_running = Window.orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_code_len_by_sku_code_len = Window.partitionBy("sku_code_len").orderBy("sku_code_len", "id")
w_sku_code_len_by_sku_code_len_2 = Window.partitionBy("sku_code_len").orderBy("sku_code_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_code_len_by_weight_kg_ru = Window.partitionBy("sku_code_len").orderBy(F.col("weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_code_len_by_unit_cost_fu = Window.partitionBy("sku_code_len").orderBy(F.col("unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_cost = Window.orderBy("unit_cost", "id")
w_all_by_rows_in_sku_code_len = Window.orderBy(F.col("rows_in_sku_code_len").desc(), "id")
w_sku_code_len_by_last_weight = Window.partitionBy("sku_code_len").orderBy(F.col("last_weight_kg").desc_nulls_first(), "id")

filtered = (
    df_skus
    .filter(~F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.min("unit_cost").over(w_sku_code_len).alias("sku_code_len_min_unit_cost"),
        F.round(F.avg("unit_cost").over(w_sku_code_len), 2).alias("sku_code_len_avg_unit_cost"),
        F.last("sku_code_len", ignorenulls=True).over(w_all_by_unit_cost_running).alias("last_known_sku_code_len"),
        F.lag("sku_code_len", 2, 0).over(w_sku_code_len_by_sku_code_len).alias("prev2_sku_code_len"),
        F.nth_value("sku_code_len", 2).over(w_sku_code_len_by_sku_code_len_2).alias("second_sku_code_len"),
        F.count(F.lit(1)).over(w_sku_code_len).alias("rows_in_sku_code_len"),
        F.last("unit_cost", ignorenulls=True).over(w_sku_code_len_by_weight_kg_ru).alias("last_known_unit_cost"),
        F.last("weight_kg").over(w_sku_code_len_by_unit_cost_fu).alias("last_weight_kg"),
        (F.col("weight_kg") - F.lag("weight_kg").over(w_all_by_unit_cost)).alias("weight_kg_change"),
        F.min("sku_code_len").over(w_sku_code_len).alias("sku_code_len_min_sku_code_len"),
    )
)

labeled = (
    with_trends
    .filter(F.col("sku_code_len_avg_unit_cost") < 320.2)
    .select(
        "*",
        F.concat(F.col("sku_code_ref"), F.lit("#"), F.col("prev2_sku_code_len").cast("string")).alias("sku_code_ref_ref"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "sku_code_ref",
        "sku_code_len_min_unit_cost",
        "last_known_sku_code_len",
        "prev2_sku_code_len",
        "second_sku_code_len",
        "last_known_unit_cost",
        "last_weight_kg",
        "weight_kg_change",
        "sku_code_len_min_sku_code_len",
        F.round(F.col("last_known_unit_cost") - F.avg("last_known_unit_cost").over(w_sku_code_len), 2).alias("last_known_unit_cost_vs_sku_code_len_avg"),
        (F.col("prev2_sku_code_len") - F.lag("prev2_sku_code_len").over(w_all_by_rows_in_sku_code_len)).alias("prev2_sku_code_len_change"),
        F.row_number().over(w_sku_code_len_by_last_weight).alias("rn"),
    )
)

result = (
    ranked
    .filter(F.col("sku_code_len_min_unit_cost") > 400.9)
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "sku_code_ref",
        "last_known_sku_code_len",
        "prev2_sku_code_len",
        "second_sku_code_len",
        "weight_kg_change",
        "sku_code_len_min_sku_code_len",
        "last_known_unit_cost_vs_sku_code_len_avg",
        "prev2_sku_code_len_change",
        F.when(F.col("second_sku_code_len") >= 9, "HIGH").when(F.col("second_sku_code_len") >= 6, "MEDIUM").otherwise("LOW").alias("second_sku_code_len_tier"),
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
    .orderBy("sku_code_len_min_sku_code_len")
)
