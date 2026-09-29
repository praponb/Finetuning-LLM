from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost = Window.orderBy(F.col("unit_cost").asc_nulls_last(), "id")
w_is_active_is_active_flag = Window.partitionBy("is_active", "is_active_flag")

prepared_skus = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        "is_active",
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
        F.lower(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
        F.substring(F.trim(F.col("category")), 1, 3).alias("category_prefix"),
    )
)

screened = (
    prepared_skus
    .crossJoin(prepared_skus.agg(F.avg("unit_cost").alias("overall_avg_unit_cost")))
    .filter(F.col("unit_cost") > F.col("overall_avg_unit_cost"))
    .drop("overall_avg_unit_cost")
)

with_windows = (
    screened
    .withColumn("unit_cost_seq", F.row_number().over(w_all_by_unit_cost))
    .withColumn("distinct_category_in_is_active_is_active_flag", F.size(F.collect_set("category").over(w_is_active_is_active_flag)))
)

labeled = (
    with_windows
    .filter(~F.col("category").isin("Apparel", "Toys"))
    .select(
        "*",
        F.when(F.col("unit_cost") >= 277.0, "A").when(F.col("unit_cost") >= 158.7, "B").otherwise("C").alias("unit_cost_tier"),
        F.coalesce(F.col("unit_cost_seq"), F.lit(0)).alias("unit_cost_seq_filled"),
        F.coalesce(F.col("unit_cost_seq"), F.lit(0)).alias("unit_cost_seq_filled_2"),
    )
)

enriched = (
    labeled
    .select(
        "*",
        F.abs(F.col("unit_cost_seq") - 4).alias("unit_cost_seq_distance"),
        F.round(F.col("is_active_flag") / F.nullif(F.col("unit_cost"), F.lit(0)), 3).alias("is_active_flag_per_unit_cost"),
        (F.col("unit_cost_seq_filled_2") - F.col("unit_cost_seq")).alias("unit_cost_seq_filled_2_minus_unit_cost_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "unit_cost_seq_filled",
        "distinct_category_in_is_active_is_active_flag",
        "unit_cost_seq",
    )
    .unpivot(
        ids=["id"],
        values=["unit_cost_seq_filled", "distinct_category_in_is_active_is_active_flag", "unit_cost_seq"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
