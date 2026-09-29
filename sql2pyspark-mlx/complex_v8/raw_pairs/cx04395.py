from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_status_active_by_n_status = Window.partitionBy("n_status_active").orderBy("n_status_active")
w_n_status_active_product = Window.partitionBy("n_status_active", "product")
w_all_by_max_end_date_week_yea = Window.orderBy("max_end_date_week_year", "product", "end_date_week_quarter_ym", "holder_segment")
w_holder_segment_n_status_acti = Window.partitionBy("holder_segment", "n_status_active").orderBy(F.col("max_end_date_week_year").desc())
w_holder_segment_n_status_acti_2 = Window.partitionBy("holder_segment", "n_status_active").orderBy(F.col("max_end_date_week_year").desc(), "product", "end_date_week_quarter_ym", "holder_segment")

filtered = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-12-02").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.end_date_week",
        "s.status_slug",
        F.col("p.segment").alias("holder_segment"),
    )
)

screened = (
    combined_details.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isin("ACTIVE", "LAPSED")) & (F.col("p.start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-05-02").cast("date"))), "left_anti")
)

annotated = (
    screened
    .select(
        "*",
        F.date_trunc("quarter", F.col("end_date_week")).alias("end_date_week_quarter"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.year(F.col("end_date_week")).alias("end_date_week_year"),
    )
)

grouped = (
    annotated
    .groupBy(
        "product",
        F.date_format("end_date_week_quarter", "yyyy-MM").alias("end_date_week_quarter_ym"),
        "holder_segment",
    )
    .agg(
        F.count(F.when(F.col("status") != "ACTIVE", 1)).alias("n_status_active"),
        F.max("end_date_week_year").alias("max_end_date_week_year"),
        F.array_sort(F.collect_set("holder_segment")).alias("holder_segment_list"),
        F.avg("end_date_week_year").alias("avg_end_date_week_year_check"),
    )
    .filter(F.col("avg_end_date_week_year_check") > 2025)
    .drop("avg_end_date_week_year_check")
)

qualified = (
    grouped
    .crossJoin(grouped.agg(F.min("max_end_date_week_year").alias("overall_min_max_end_date_week_year")))
    .filter(F.col("max_end_date_week_year") > F.col("overall_min_max_end_date_week_year"))
    .drop("overall_min_max_end_date_week_year")
)

ranked = (
    qualified
    .select(
        "product",
        "end_date_week_quarter_ym",
        "holder_segment",
        "max_end_date_week_year",
        "holder_segment_list",
        F.round(F.percent_rank().over(w_n_status_active_by_n_status), 3).alias("n_status_active_percent_rank"),
        F.count(F.lit(1)).over(w_n_status_active_product).alias("rows_in_n_status_active_product"),
        F.lead("max_end_date_week_year").over(w_all_by_max_end_date_week_yea).alias("next_max_end_date_week_year"),
        F.dense_rank().over(w_holder_segment_n_status_acti).alias("max_end_date_week_year_dense_rank"),
        F.lead("max_end_date_week_year", 1, 0).over(w_holder_segment_n_status_acti_2).alias("next_max_end_date_week_year_2"),
    )
)

result = (
    ranked
    .select(
        "product",
        "end_date_week_quarter_ym",
        F.col("rows_in_n_status_active_product").alias("m_rows_in_n_status_active_product"),
        F.col("next_max_end_date_week_year").alias("m_next_max_end_date_week_year"),
        F.col("max_end_date_week_year").alias("m_max_end_date_week_year"),
        F.col("next_max_end_date_week_year_2").alias("next_max_end_date_week_year_2_v"),
    )
    .unpivot(
        ids=["product", "end_date_week_quarter_ym"],
        values=["m_rows_in_n_status_active_product", "m_next_max_end_date_week_year", "m_max_end_date_week_year", "next_max_end_date_week_year_2_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("product", "end_date_week_quarter_ym", "field")
)
