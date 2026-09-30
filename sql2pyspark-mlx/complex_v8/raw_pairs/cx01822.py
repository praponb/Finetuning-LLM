from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status = Window.partitionBy("product", "status")
w_status_holder_region_by_end = Window.partitionBy("status", "holder_region").orderBy("end_date", "id")
w_status_holder_region_by_end_2 = Window.partitionBy("status", "holder_region").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)
w_status_holder_region_by_end_3 = Window.partitionBy("status", "holder_region").orderBy("end_date", "id").rowsBetween(-1, -1)

recent = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("premium") > 2111.3))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2111.3).alias("premium_distance"),
    )
)

enriched_holder = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", F.col("p.region").alias("holder_region"))
)

with_windows = (
    enriched_holder
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        "premium_distance",
        "holder_region",
        F.round(F.col("premium") - F.avg("premium").over(w_product_status), 2).alias("premium_vs_product_status_avg"),
        F.lead("premium_distance", 2).over(w_status_holder_region_by_end).alias("next2_premium_distance"),
        F.count(F.lit(1)).over(w_product_status).alias("rows_in_product_status"),
        F.max("premium_distance").over(w_status_holder_region_by_end_2).alias("premium_distance_max_4"),
        F.rank().over(w_status_holder_region_by_end).alias("end_date_rank"),
        F.size(F.collect_set("holder_id").over(w_product_status)).alias("distinct_holder_id_in_product_status"),
        F.sum("premium").over(w_status_holder_region_by_end_3).alias("premium_sum_1_prior"),
    )
)

top_rows = with_windows.filter((F.col("end_date_rank") <= 2) & (F.year(F.col("end_date")) == 2026))

result = (
    top_rows
    .select(
        "id",
        "premium",
        "end_date",
        "premium_distance",
        "holder_region",
        "next2_premium_distance",
        "rows_in_product_status",
        "premium_distance_max_4",
        "distinct_holder_id_in_product_status",
        "premium_sum_1_prior",
        F.when(F.col("premium") >= 2945.9, "large").when(F.col("premium") >= 2033.8, "medium").otherwise("small").alias("premium_tier"),
        F.greatest(F.col("premium_sum_1_prior"), F.col("next2_premium_distance")).alias("greatest_premium_sum_1_prior_next2_premium_dista"),
    )
    .orderBy(F.col("premium").desc())
)
