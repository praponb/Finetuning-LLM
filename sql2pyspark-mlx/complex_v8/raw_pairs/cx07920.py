from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_holder_joined_on_m = Window.partitionBy("holder_id", F.month("holder_joined_on")).orderBy(F.col("start_date_month_end").desc_nulls_first())
w_holder_id_holder_joined_on_m_2 = Window.partitionBy("holder_id", F.month("holder_joined_on")).orderBy(F.col("start_date_month_end").desc_nulls_first(), "id")
w_holder_id_holder_joined_on_m_3 = Window.partitionBy("holder_id", F.month("holder_joined_on")).orderBy(F.col("start_date_month_end").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_holder_id_holder_joined_on_m_4 = Window.partitionBy("holder_id", F.month("holder_joined_on")).orderBy(F.col("start_date_month_end").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_end_date_year_start_date_mon = Window.partitionBy(F.year("end_date"), F.month("start_date"))
w_product_product_or_unknown_b = Window.partitionBy("product", "product_or_unknown").orderBy(F.col("prev2_holder_joined_on").desc(), "id")

cleaned = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.year(F.col("end_date")).alias("end_date_year"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.end_date_year",
        "s.product_or_unknown",
        "s.start_date_month_end",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

tagged = (
    joined
    .filter(F.col("end_date").isNotNull())
    .withColumn("end_date_year_distance", F.abs(F.col("end_date_year") - 2025))
)

metrics = (
    tagged
    .select(
        "*",
        F.round(F.cume_dist().over(w_holder_id_holder_joined_on_m), 3).alias("start_date_month_end_cume_dist"),
        F.lag("holder_joined_on", 2).over(w_holder_id_holder_joined_on_m_2).alias("prev2_holder_joined_on"),
        F.max("end_date_year").over(w_holder_id_holder_joined_on_m_3).alias("end_date_year_max_2"),
        F.round(F.avg("end_date_year").over(w_holder_id_holder_joined_on_m_4), 2).alias("end_date_year_avg_2_prior"),
        F.lag("end_date_year_distance", 3).over(w_holder_id_holder_joined_on_m_2).alias("prev3_end_date_year_distance"),
        F.lead("end_date_year_distance", 1, 0).over(w_holder_id_holder_joined_on_m_2).alias("next_end_date_year_distance"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "product_or_unknown",
        "holder_joined_on",
        "end_date_year_distance",
        "end_date_year_max_2",
        "end_date_year_avg_2_prior",
        "next_end_date_year_distance",
        F.size(F.collect_set("product_or_unknown").over(w_end_date_year_start_date_mon)).alias("distinct_product_or_unknown_in_end_date_year_sta"),
        F.row_number().over(w_product_product_or_unknown_b).alias("rn"),
        F.min("prev3_end_date_year_distance").over(w_end_date_year_start_date_mon).alias("end_date_year_start_date_month_min_prev3_end_dat"),
    )
)

result = (
    with_trends
    .groupBy("holder_id", "rn")
    .pivot("product_or_unknown", ["TRAVEL", "AUTO", "HEALTH", "LIFE"])
    .agg(F.max("end_date_year_start_date_month_min_prev3_end_dat"))
    .orderBy("holder_id", "rn")
)
