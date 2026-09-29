from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_label_is_active_by = Window.partitionBy("is_active_label", "is_active").orderBy(F.col("row_count").desc_nulls_first(), "is_active_flag", "labels_count", "is_active").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_labels_count_is_active_label = Window.partitionBy("labels_count", "is_active_label")
w_is_active_label_is_active_by_2 = Window.partitionBy("is_active_label", "is_active").orderBy(F.col("row_count").desc_nulls_first(), "is_active_flag", "labels_count", "is_active")
w_is_active_row_count_quartile = Window.partitionBy("is_active", "row_count_quartile").orderBy("rows_in_labels_count_is_active_label", "is_active_flag", "labels_count", "is_active")
w_all_by_row_count_vs_labels_c = Window.orderBy("row_count_vs_labels_count_is_active_label_avg", "is_active_flag", "labels_count", "is_active")
w_is_active_row_count_quartile_2 = Window.partitionBy("is_active", "row_count_quartile").orderBy("rows_in_labels_count_is_active_label")

filtered = (
    df_skus
    .filter(F.lower(F.col("sku_code")).like("%-2%"))
    .select(
        "id",
        "sku_code",
        "labels",
        "is_active",
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
        F.size(F.col("labels")).alias("labels_count"),
    )
)

per_group = (
    filtered
    .groupBy("is_active_flag", "labels_count", "is_active")
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("sku_code"))).like("A%"), F.col("labels_count")).otherwise(0)).alias("labels_count_upper_trim_sku_code_like_a"),
        F.count("*").alias("row_count"),
    )
)

annotated = (
    per_group
    .filter(F.col("labels_count") >= 1)
    .select(
        "is_active_flag",
        "labels_count",
        "is_active",
        "labels_count_upper_trim_sku_code_like_a",
        "row_count",
        F.round(F.col("is_active_flag") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("is_active_flag_per_row_count"),
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.last("is_active_flag_per_row_count", ignorenulls=True).over(w_is_active_label_is_active_by).alias("last_known_is_active_flag_per_row_count"),
        F.count(F.lit(1)).over(w_labels_count_is_active_label).alias("rows_in_labels_count_is_active_label"),
        F.ntile(4).over(w_is_active_label_is_active_by_2).alias("row_count_quartile"),
        F.round(F.col("row_count") - F.avg("row_count").over(w_labels_count_is_active_label), 2).alias("row_count_vs_labels_count_is_active_label_avg"),
        F.round(F.percent_rank().over(w_is_active_label_is_active_by_2), 3).alias("row_count_percent_rank"),
        (F.col("row_count") - F.lag("row_count").over(w_is_active_label_is_active_by_2)).alias("row_count_change"),
    )
)

with_windows = (
    ranked
    .select(
        "is_active_flag",
        "labels_count",
        "is_active",
        "row_count",
        "is_active_label",
        "row_count_quartile",
        "row_count_vs_labels_count_is_active_label_avg",
        "row_count_change",
        F.row_number().over(w_is_active_row_count_quartile).alias("rn"),
        F.row_number().over(w_all_by_row_count_vs_labels_c).alias("seq_no"),
        F.dense_rank().over(w_is_active_row_count_quartile_2).alias("rows_in_labels_count_is_active_label_dense_rank"),
    )
)

scored = with_windows.withColumn("row_count_change_minus_rn", F.col("row_count_change") - F.col("rn"))

result = (
    scored
    .filter(F.col("row_count_change_minus_rn") > 2)
    .select("labels_count")
    .distinct()
    .orderBy(F.col("labels_count").asc())
)
