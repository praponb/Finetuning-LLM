from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_region_holder_id_by_e = Window.partitionBy("holder_region", "holder_id").orderBy(F.col("end_date").desc(), "id")
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_holder_id_by_end_date_full = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_product_by_end_dat = Window.partitionBy("holder_id", "product").orderBy(F.col("end_date").desc(), "id")
w_holder_id_product_by_end_dat_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_product_by_end_dat_3 = Window.partitionBy("holder_id", "product").orderBy(F.col("end_date").desc())
w_holder_region_holder_id_by_e_2 = Window.partitionBy("holder_region", "holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date = Window.orderBy("end_date", "id")
w_n_product_health_last_start = Window.partitionBy("n_product_health", "last_start_date_year").orderBy(F.col("next_end_date_2_span_days").desc(), "first_known_start_date_ym", "end_date_ym", "last_start_date_year").rowsBetween(-1, -1)
w_n_product_health = Window.partitionBy("n_product_health")
w_n_product_health_last_start_2 = Window.partitionBy("n_product_health", "last_start_date_year").orderBy(F.col("next_end_date_2_span_days").desc(), "first_known_start_date_ym", "end_date_ym", "last_start_date_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_policies
    .filter((F.col("product") == "AUTO") & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.last("start_date").over(w_all_by_start_date_full).alias("last_start_date"),
        F.row_number().over(w_holder_region_holder_id_by_e).alias("rn"),
        F.ntile(10).over(w_all_by_start_date).alias("start_date_decile"),
        F.nth_value("end_date", 2).over(w_holder_id_by_end_date_full).alias("second_end_date"),
        F.first("start_date", ignorenulls=True).over(w_all_by_start_date_running).alias("first_known_start_date"),
        F.lead("end_date").over(w_holder_id_product_by_end_dat).alias("next_end_date"),
        F.ntile(4).over(w_all_by_start_date).alias("start_date_quartile"),
        F.nth_value("end_date", 2).over(w_holder_id_product_by_end_dat_2).alias("second_end_date_2"),
        F.round(F.percent_rank().over(w_holder_id_product_by_end_dat_3), 3).alias("end_date_percent_rank"),
        F.lead("end_date").over(w_all_by_start_date).alias("next_end_date_2"),
        F.nth_value("start_date", 2).over(w_holder_region_holder_id_by_e_2).alias("second_start_date"),
        F.ntile(4).over(w_all_by_end_date).alias("end_date_quartile"),
        F.lead("end_date", 3).over(w_holder_id_product_by_end_dat).alias("next3_end_date"),
        F.round(F.percent_rank().over(w_holder_region_holder_id_by_e), 3).alias("end_date_percent_rank_2"),
    )
)

first_rows = metrics.filter(F.col("rn") <= 3)

totals = (
    first_rows
    .groupBy(
        F.date_format("first_known_start_date", "yyyy-MM").alias("first_known_start_date_ym"),
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
        F.year("last_start_date").alias("last_start_date_year"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("holder_name"))) >= 4, 1)) / F.count("*"), 1).alias("pct_length_trim_holder_name_4"),
        F.sum(F.when(F.col("product") == "HEALTH", 1).otherwise(0)).alias("n_product_health"),
        F.datediff(F.max("next_end_date_2"), F.min("next_end_date_2")).alias("next_end_date_2_span_days"),
    )
)

sequenced = (
    totals
    .withColumn("last_start_date_year_avg_1_prior", F.round(F.avg("last_start_date_year").over(w_n_product_health_last_start), 2))
    .withColumn("max_next_end_date_2_span_days_so_far", F.max("next_end_date_2_span_days").over(w_n_product_health_last_start_2))
)

result = (
    sequenced
    .select(
        "first_known_start_date_ym",
        "end_date_ym",
        "last_start_date_year",
        "n_product_health",
        "next_end_date_2_span_days",
        "last_start_date_year_avg_1_prior",
        "max_next_end_date_2_span_days_so_far",
        F.initcap(F.trim(F.col("end_date_ym"))).alias("end_date_ym_clean"),
        F.least(F.col("n_product_health"), F.col("last_start_date_year")).alias("least_n_product_health_last_start_date_year"),
    )
    .orderBy("end_date_ym")
)
