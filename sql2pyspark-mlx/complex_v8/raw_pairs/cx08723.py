from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_max_status_len_holder_id_by = Window.partitionBy("max_status_len", "holder_id").orderBy("holder_joined_on", "holder_id", "product")
w_max_status_len_holder_id_by_2 = Window.partitionBy("max_status_len", "holder_id").orderBy("holder_joined_on", "holder_id", "product").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_joined_on_2_month = Window.partitionBy(F.month("holder_joined_on_2"))
w_holder_id_by_max_status_len = Window.partitionBy("holder_id").orderBy("max_status_len", "holder_id", "product")
w_holder_id_by_max_status_len_2 = Window.partitionBy("holder_id").orderBy("max_status_len", "holder_id", "product").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_max_status_len_3 = Window.partitionBy("holder_id").orderBy("max_status_len")
w_holder_id = Window.partitionBy("holder_id")
w_holder_joined_on_quartile_by = Window.partitionBy("holder_joined_on_quartile").orderBy("holder_joined_on_2", "holder_id", "product")
w_holder_joined_on_quartile_by_2 = Window.partitionBy("holder_joined_on_quartile").orderBy("holder_joined_on_2", "holder_id", "product").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_max_status_len_quart = Window.partitionBy("product", "max_status_len_quartile").orderBy(F.col("first_known_holder_joined_on_2").desc())

cleaned = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-03-13").cast("date")) & (F.year(F.col("start_date")) == 2025))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

stats = (
    cleaned
    .groupBy("holder_id", "product")
    .agg(
        F.count("*").alias("row_count"),
        F.max("status_len").alias("max_status_len"),
    )
)

with_parent_holder = (
    stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.year(F.col("p.joined_on")) == 2024)
    .select(
        "s.holder_id",
        "s.product",
        "s.max_status_len",
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

enriched_holder = (
    with_parent_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.holder_id",
        "s.product",
        "s.max_status_len",
        "s.holder_joined_on",
        "s.holder_name",
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

with_trends = (
    enriched_holder
    .select(
        "*",
        F.ntile(4).over(w_max_status_len_holder_id_by).alias("holder_joined_on_quartile"),
        F.max("max_status_len").over(w_max_status_len_holder_id_by_2).alias("max_max_status_len_so_far"),
        F.count(F.lit(1)).over(w_holder_joined_on_2_month).alias("rows_in_holder_joined_on_2_month"),
        F.round(F.percent_rank().over(w_max_status_len_holder_id_by), 3).alias("holder_joined_on_percent_rank"),
    )
)

with_windows = (
    with_trends
    .select(
        "holder_id",
        "product",
        "holder_joined_on",
        "holder_name",
        "holder_joined_on_2",
        "holder_joined_on_quartile",
        "max_max_status_len_so_far",
        "rows_in_holder_joined_on_2_month",
        "holder_joined_on_percent_rank",
        F.lead("holder_joined_on_quartile", 3).over(w_holder_id_by_max_status_len).alias("next3_holder_joined_on_quartile"),
        F.ntile(4).over(w_holder_id_by_max_status_len).alias("max_status_len_quartile"),
        F.first("holder_joined_on_2", ignorenulls=True).over(w_holder_id_by_max_status_len_2).alias("first_known_holder_joined_on_2"),
        F.round(F.cume_dist().over(w_holder_id_by_max_status_len_3), 3).alias("max_status_len_cume_dist"),
    )
)

ranked = (
    with_windows
    .select(
        "holder_id",
        "product",
        "holder_joined_on",
        "holder_name",
        "holder_joined_on_2",
        "holder_joined_on_quartile",
        "max_max_status_len_so_far",
        "rows_in_holder_joined_on_2_month",
        "holder_joined_on_percent_rank",
        "next3_holder_joined_on_quartile",
        "first_known_holder_joined_on_2",
        "max_status_len_cume_dist",
        F.min("max_status_len_cume_dist").over(w_holder_id).alias("holder_id_min_max_status_len_cume_dist"),
        (F.col("max_max_status_len_so_far") - F.lag("max_max_status_len_so_far").over(w_holder_joined_on_quartile_by)).alias("max_max_status_len_so_far_change"),
        F.max("holder_joined_on_quartile").over(w_holder_id).alias("holder_id_max_holder_joined_on_quartile"),
        F.last("max_status_len_cume_dist", ignorenulls=True).over(w_holder_joined_on_quartile_by_2).alias("last_known_max_status_len_cume_dist"),
        F.round(F.cume_dist().over(w_product_max_status_len_quart), 3).alias("first_known_holder_joined_on_2_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "holder_id",
        "product",
        "holder_name",
        "holder_joined_on_2",
        "holder_joined_on_quartile",
        "rows_in_holder_joined_on_2_month",
        "holder_joined_on_percent_rank",
        "first_known_holder_joined_on_2",
        "max_status_len_cume_dist",
        "holder_id_min_max_status_len_cume_dist",
        "max_max_status_len_so_far_change",
        "holder_id_max_holder_joined_on_quartile",
        "last_known_max_status_len_cume_dist",
    )
    .orderBy(F.col("rows_in_holder_joined_on_2_month").desc())
)
