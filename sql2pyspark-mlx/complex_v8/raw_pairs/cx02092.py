from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active = Window.partitionBy("is_active")
w_is_active_by_weight_kg_runni = Window.partitionBy("is_active").orderBy(F.col("weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_active_by_weight_kg_movin = Window.partitionBy("is_active").orderBy(F.col("weight_kg").desc(), "id").rowsBetween(-3, Window.currentRow)
w_is_active_by_weight_kg = Window.partitionBy("is_active").orderBy(F.col("weight_kg").asc_nulls_last(), "id")
w_all_by_weight_kg_full = Window.orderBy(F.col("weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_active_by_weight_kg_2 = Window.partitionBy("is_active").orderBy(F.col("weight_kg").desc(), "id")
w_all_by_weight_kg_ties = Window.orderBy(F.col("weight_kg").desc())
w_all_by_first_known_weight_kg = Window.orderBy("first_known_weight_kg", "id")
w_all_by_weight_kg = Window.orderBy(F.col("weight_kg").desc(), "id")

base_skus = (
    df_skus
    .filter((F.size(F.col("labels")) >= 2) & (F.col("unit_cost") <= 330.6))
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "is_active",
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
        F.upper(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)

ranked = (
    base_skus
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "sku_code_ref",
        "sku_code_clean",
        F.round(F.col("weight_kg") / F.nullif(F.sum("weight_kg").over(w_is_active), F.lit(0)), 4).alias("weight_kg_share_of_is_active"),
        F.round(F.col("weight_kg") - F.avg("weight_kg").over(w_is_active), 2).alias("weight_kg_vs_is_active_avg"),
        F.first("weight_kg", ignorenulls=True).over(w_is_active_by_weight_kg_runni).alias("first_known_weight_kg"),
        F.round(F.avg("weight_kg").over(w_is_active_by_weight_kg_movin), 2).alias("weight_kg_avg_4"),
        F.lag("weight_kg").over(w_is_active_by_weight_kg).alias("prev_weight_kg"),
        F.nth_value("weight_kg", 2).over(w_all_by_weight_kg_full).alias("second_weight_kg"),
        F.lag("weight_kg", 1, 0).over(w_is_active_by_weight_kg_2).alias("prev_weight_kg_2"),
        F.row_number().over(w_is_active_by_weight_kg_2).alias("seq_no"),
        F.count(F.lit(1)).over(w_is_active).alias("rows_in_is_active"),
        F.sum("weight_kg").over(w_is_active).alias("is_active_sum_weight_kg"),
    )
)

top_rows = (
    ranked
    .filter(F.col("seq_no") <= 5)
    .drop("seq_no")
)

windowed = (
    top_rows
    .select(
        "*",
        F.dense_rank().over(w_all_by_weight_kg_ties).alias("weight_kg_dense_rank"),
        F.ntile(5).over(w_all_by_first_known_weight_kg).alias("first_known_weight_kg_quintile"),
        F.round(F.cume_dist().over(w_all_by_weight_kg), 3).alias("weight_kg_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "sku_code_ref",
        "sku_code_clean",
        "weight_kg_vs_is_active_avg",
        "first_known_weight_kg",
        "weight_kg_avg_4",
        "prev_weight_kg",
        "prev_weight_kg_2",
        "rows_in_is_active",
        "is_active_sum_weight_kg",
        "first_known_weight_kg_quintile",
        "weight_kg_cume_dist",
    )
    .orderBy("weight_kg_cume_dist")
)
