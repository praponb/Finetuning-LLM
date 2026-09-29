from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_quarter = Window.partitionBy("end_date_quarter")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_start_date = Window.orderBy("start_date", "id")
w_distinct_product_in_end_date = Window.partitionBy("distinct_product_in_end_date_quarter", "product").orderBy("premium", "id")
w_all_by_distinct_product_in_e = Window.orderBy(F.col("distinct_product_in_end_date_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_policies
    .filter((F.col("start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-05-30").cast("date"))) & (F.col("product").isin("HOME", "HEALTH", "LIFE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.quarter(F.col("end_date")).alias("end_date_quarter"),
        F.add_months(F.col("end_date"), 6).alias("end_date_plus_6m"),
    )
)

annotated = (
    cleaned
    .select(
        "*",
        F.add_months(F.col("start_date"), 3).alias("start_date_plus_3m"),
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_end_date_quarter)).alias("distinct_holder_id_in_end_date_quarter"),
        F.size(F.collect_set("product").over(w_end_date_quarter)).alias("distinct_product_in_end_date_quarter"),
        F.ntile(10).over(w_all_by_end_date).alias("end_date_decile"),
        F.lead("premium_adj", 3).over(w_all_by_start_date).alias("next3_premium_adj"),
        F.min("premium").over(w_end_date_quarter).alias("end_date_quarter_min_premium"),
        F.max("premium_adj").over(w_end_date_quarter).alias("end_date_quarter_max_premium_adj"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.round(F.cume_dist().over(w_distinct_product_in_end_date), 3).alias("premium_cume_dist"),
        F.nth_value("end_date_quarter_min_premium", 2).over(w_all_by_distinct_product_in_e).alias("second_end_date_quarter_min_premium"),
        F.count(F.lit(1)).over(w_end_date_quarter).alias("rows_in_end_date_quarter"),
        F.ntile(10).over(w_distinct_product_in_end_date).alias("premium_decile"),
    )
)

result = (
    with_windows
    .groupBy("end_date_quarter")
    .pivot("status", ["CANCELLED", "LAPSED", "EXPIRED", "ACTIVE"])
    .agg(F.min("end_date_quarter_min_premium"))
)
