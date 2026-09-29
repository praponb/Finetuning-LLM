from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_by_row_count_movin = Window.partitionBy("is_active").orderBy("row_count", "is_active").rowsBetween(-6, Window.currentRow)
w_row_count = Window.partitionBy("row_count")
w_row_count_is_active_by_row_c = Window.partitionBy("row_count", "is_active").orderBy(F.col("row_count").desc(), "is_active").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_active_by_row_count = Window.partitionBy("is_active").orderBy("row_count", "is_active")

cleaned = (
    df_skus
    .filter(F.col("category") == "Electronics")
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "labels",
        "is_active",
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
        (F.floor(F.col("weight_kg") / 5) * 5).alias("weight_kg_bucket"),
    )
)

stats = (
    cleaned
    .filter(F.col("sku_code").isNotNull())
    .groupBy("is_active")
    .agg(
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.count("*").alias("row_count"),
        F.max("weight_kg_bucket").alias("max_weight_kg_bucket_check"),
    )
    .filter(F.col("max_weight_kg_bucket_check") > 3)
    .drop("max_weight_kg_bucket_check")
)

ranked = (
    stats
    .select(
        "*",
        F.round(F.avg("row_count").over(w_is_active_by_row_count_movin), 2).alias("row_count_avg_7"),
        F.count(F.lit(1)).over(w_row_count).alias("rows_in_row_count"),
        F.nth_value("row_count", 2).over(w_row_count_is_active_by_row_c).alias("second_row_count"),
        F.round(F.col("row_count") / F.nullif(F.sum("row_count").over(w_row_count), F.lit(0)), 4).alias("row_count_share_of_row_count"),
        F.row_number().over(w_is_active_by_row_count).alias("seq_no"),
    )
)

qualified = (
    ranked
    .crossJoin(ranked.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

flattened = (
    qualified
    .select(
        "is_active",
        "row_count",
        "row_count_avg_7",
        "rows_in_row_count",
        "second_row_count",
        "row_count_share_of_row_count",
        "seq_no",
        F.explode_outer("sku_code_list").alias("sku_code_list_item"),
    )
)

annotated = (
    flattened
    .select(
        "is_active",
        "row_count",
        "row_count_avg_7",
        "rows_in_row_count",
        "second_row_count",
        "seq_no",
        "sku_code_list_item",
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
        F.round(F.col("second_row_count") / F.nullif(F.col("rows_in_row_count"), F.lit(0)), 3).alias("second_row_count_per_rows_in_row_count"),
    )
)

filtered = (
    annotated
    .crossJoin(annotated.agg(F.min("is_active_flag").alias("overall_min_is_active_flag")))
    .filter(F.col("is_active_flag") > F.col("overall_min_is_active_flag"))
    .drop("overall_min_is_active_flag")
)

result = (
    filtered
    .select(
        "is_active",
        "row_count",
        "row_count_avg_7",
        "rows_in_row_count",
        "second_row_count",
        "seq_no",
        "sku_code_list_item",
        "is_active_flag",
        F.round(F.col("row_count_avg_7") * 1.1, 2).alias("row_count_avg_7_adj"),
    )
)
