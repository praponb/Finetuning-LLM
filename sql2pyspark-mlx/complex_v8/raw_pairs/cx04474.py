from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id_by_end_dat = Window.partitionBy("product", "holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id_by_end_dat_2 = Window.partitionBy("product", "holder_id").orderBy("end_date", "id")
w_product_holder_id_by_end_dat_3 = Window.partitionBy("product", "holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_end_date_month = Window.partitionBy("holder_id", F.month("end_date"))
w_end_date_month_holder_id_by = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id").orderBy(F.col("first_known_end_date").asc_nulls_last())
w_end_date_month_holder_segmen = Window.partitionBy(F.month("end_date"), "holder_segment").orderBy(F.col("second_premium").asc_nulls_last(), "id")

base_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-07-19").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

with_trends = (
    base_policys
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        F.nth_value("end_date", 2).over(w_product_holder_id_by_end_dat).alias("second_end_date"),
        F.nth_value("premium", 2).over(w_product_holder_id_by_end_dat).alias("second_premium"),
        F.rank().over(w_product_holder_id_by_end_dat_2).alias("end_date_rank"),
        F.first("end_date", ignorenulls=True).over(w_product_holder_id_by_end_dat_3).alias("first_known_end_date"),
    )
)

first_rows = (
    with_trends
    .filter(F.col("end_date_rank") <= 5)
    .drop("end_date_rank")
)

combined_details_holder = (
    first_rows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.second_end_date",
        "s.second_premium",
        "s.first_known_end_date",
        F.col("p.segment").alias("holder_segment"),
    )
)

windowed = (
    combined_details_holder
    .select(
        "*",
        F.size(F.collect_set("holder_segment").over(w_holder_id_end_date_month)).alias("distinct_holder_segment_in_holder_id_end_date_mo"),
        F.dense_rank().over(w_end_date_month_holder_id_by).alias("first_known_end_date_dense_rank"),
        F.count(F.lit(1)).over(w_holder_id_end_date_month).alias("rows_in_holder_id_end_date_month"),
        F.lead("end_date").over(w_end_date_month_holder_segmen).alias("next_end_date"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "second_end_date",
        "second_premium",
        "distinct_holder_segment_in_holder_id_end_date_mo",
        "first_known_end_date_dense_rank",
        "rows_in_holder_id_end_date_month",
        F.concat(F.col("holder_segment"), F.lit("#"), F.col("id").cast("string")).alias("holder_segment_ref"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("second_end_date")).alias("days_since_second_end_date"),
    )
    .orderBy(
        F.col("distinct_holder_segment_in_holder_id_end_date_mo").asc_nulls_last(),
    )
)
