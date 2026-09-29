from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_weight_kg = Window.orderBy("weight_kg", "id")
w_category = Window.partitionBy("category")
w_all_by_weight_kg_running = Window.orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_weight_kg_prior_1 = Window.orderBy("weight_kg", "id").rowsBetween(-1, -1)
w_is_active_by_first_known_wei = Window.partitionBy("is_active").orderBy(F.col("first_known_weight_kg").desc(), "id")
w_is_active_category = Window.partitionBy("is_active", "category")
w_is_active_by_weight_kg_prior = Window.partitionBy("is_active").orderBy("weight_kg", "id").rowsBetween(-6, -1)
w_category_is_active = Window.partitionBy("category", "is_active")
w_category_by_first_known_weig = Window.partitionBy("category").orderBy("first_known_weight_kg", "id").rowsBetween(-3, Window.currentRow)

base_skus = (
    df_skus
    .filter(F.col("weight_kg") >= 12.4)
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "is_active",
        F.substring(F.trim(F.col("category")), 1, 2).alias("category_prefix"),
        F.initcap(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)

ranked = (
    base_skus
    .select(
        "id",
        "category",
        "weight_kg",
        "is_active",
        "category_prefix",
        "sku_code_clean",
        (F.col("weight_kg") - F.lag("weight_kg").over(w_all_by_weight_kg)).alias("weight_kg_change"),
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
        F.first("weight_kg", ignorenulls=True).over(w_all_by_weight_kg_running).alias("first_known_weight_kg"),
        F.round(F.avg("weight_kg").over(w_all_by_weight_kg_prior_1), 2).alias("weight_kg_avg_1_prior"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.warehouse_id").isNotNull()) & (F.col("st.unit_price").between(152.7, 329.7)), "left_anti")
)

windowed = (
    with_activity
    .select(
        "*",
        F.lag("weight_kg").over(w_is_active_by_first_known_wei).alias("prev_weight_kg"),
        F.max("weight_kg_avg_1_prior").over(w_is_active_category).alias("is_active_category_max_weight_kg_avg_1_prior"),
        F.ntile(10).over(w_is_active_by_first_known_wei).alias("first_known_weight_kg_decile"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "category",
        "is_active",
        "category_prefix",
        "weight_kg_change",
        "prev_weight_kg",
        "is_active_category_max_weight_kg_avg_1_prior",
        F.round(F.avg("weight_kg_change").over(w_is_active_by_weight_kg_prior), 2).alias("weight_kg_change_avg_6_prior"),
        F.count(F.lit(1)).over(w_category_is_active).alias("rows_in_category_is_active"),
        F.round(F.avg("weight_kg_avg_1_prior").over(w_category_by_first_known_weig), 2).alias("weight_kg_avg_1_prior_avg_4"),
    )
)

derived = (
    with_windows
    .select(
        "id",
        "category",
        "is_active",
        "category_prefix",
        "weight_kg_change",
        "is_active_category_max_weight_kg_avg_1_prior",
        "weight_kg_change_avg_6_prior",
        "rows_in_category_is_active",
        "weight_kg_avg_1_prior_avg_4",
        F.length(F.trim(F.col("category"))).alias("category_len"),
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
    )
)

result = (
    derived
    .filter(F.col("category_prefix").isNotNull())
    .select(
        "id",
        "is_active",
        "category_prefix",
        "weight_kg_change",
        "weight_kg_change_avg_6_prior",
        "rows_in_category_is_active",
        "category_len",
        F.abs(F.col("category_len") - 6).alias("category_len_distance"),
    )
    .orderBy(F.col("category_prefix").desc_nulls_first())
)
