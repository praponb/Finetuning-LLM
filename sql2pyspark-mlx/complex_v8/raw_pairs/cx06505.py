from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id = Window.partitionBy("product", "holder_id")
w_start_date_month_holder_id_b = Window.partitionBy(F.month("start_date"), "holder_id").orderBy(F.col("start_date").desc_nulls_first(), "id")

cleaned = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-05-30").cast("date")) | (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "premium_distance",
        F.max("premium_distance").over(w_product_holder_id).alias("product_holder_id_max_premium_distance"),
        F.row_number().over(w_start_date_month_holder_id_b).alias("seq_no"),
        F.round(F.col("premium_distance") / F.nullif(F.sum("premium_distance").over(w_product_holder_id), F.lit(0)), 4).alias("premium_distance_share_of_product_holder_id"),
        F.lead("start_date").over(w_start_date_month_holder_id_b).alias("next_start_date"),
        F.min("premium_distance").over(w_product_holder_id).alias("product_holder_id_min_premium_distance"),
    )
)

grouped = (
    metrics
    .groupBy(
        "status",
        "seq_no",
        F.year("next_start_date").alias("next_start_date_year"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("product").isNotNull(), 1)) / F.count("*"), 1).alias("pct_product_is_not_null"),
        F.countDistinct("product").alias("distinct_products"),
        F.datediff(F.max("next_start_date"), F.min("next_start_date")).alias("next_start_date_span_days"),
        F.round(F.avg("premium_distance_share_of_product_holder_id"), 2).alias("avg_premium_distance_share_of_product_holder_id"),
    )
)

result = (
    grouped
    .select(
        "status",
        "seq_no",
        "avg_premium_distance_share_of_product_holder_id",
        "pct_product_is_not_null",
    )
    .unpivot(
        ids=["status", "seq_no"],
        values=["avg_premium_distance_share_of_product_holder_id", "pct_product_is_not_null"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
