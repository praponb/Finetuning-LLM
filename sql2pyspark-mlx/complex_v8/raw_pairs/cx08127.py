from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_holder_id_product_len = Window.partitionBy("holder_id", "product_len")
w_product_len_by_days_since_st = Window.partitionBy("product_len").orderBy(F.col("days_since_start_date_filled").asc_nulls_last(), "id")

valid = (
    df_policies
    .filter(F.col("product").isin("HEALTH", "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

unique_rows = (
    valid
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated = (
    unique_rows
    .filter(F.col("product_len") >= 6)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "product_len",
        "days_since_start_date",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.coalesce(F.col("days_since_start_date"), F.lit(0)).alias("days_since_start_date_filled"),
        F.concat(F.col("product"), F.lit("#"), F.col("product_len").cast("string")).alias("product_ref"),
    )
)

sequenced = (
    annotated
    .select(
        "*",
        F.round(F.col("days_since_start_date") - F.avg("days_since_start_date").over(w_holder_id_product_len), 2).alias("days_since_start_date_vs_holder_id_product_len_a"),
        F.round(F.avg("days_since_start_date").over(w_holder_id_product_len), 2).alias("holder_id_product_len_avg_days_since_start_date"),
        F.row_number().over(w_product_len_by_days_since_st).alias("rn"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") >= 0.2), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.product_len",
        "s.product_slug",
        "s.days_since_start_date_filled",
        "s.product_ref",
        "s.days_since_start_date_vs_holder_id_product_len_a",
        "s.holder_id_product_len_avg_days_since_start_date",
        "s.rn",
        F.col("p.segment").alias("holder_segment"),
    )
)

grouped = (
    combined_details
    .groupBy("product_len", "holder_segment")
    .agg(
        F.datediff(F.max("start_date"), F.min("start_date")).alias("start_date_span_days"),
        F.max("days_since_start_date_filled").alias("max_days_since_start_date_filled"),
        F.sum("rn").alias("sum_rn_check"),
    )
    .filter(F.col("sum_rn_check") > 2)
    .drop("sum_rn_check")
)

result = (
    grouped
    .filter(F.col("start_date_span_days") <= 630)
    .select("product_len", "holder_segment", "start_date_span_days")
)
