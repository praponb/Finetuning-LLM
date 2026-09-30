from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_holder_id_by_e = Window.partitionBy("holder_region", "holder_id").orderBy(F.col("end_date").desc())
w_holder_region_holder_id_by_e_2 = Window.partitionBy("holder_region", "holder_id").orderBy(F.col("end_date").desc(), "id")
w_end_date_month_product_by_en = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_product_by_en_2 = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("end_date").desc())
w_end_date_month_product_by_en_3 = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("end_date").desc(), "id")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_holder_region_holder_id_by_e_3 = Window.partitionBy("holder_region", "holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_rank_by_end_date_de = Window.partitionBy("end_date_rank").orderBy(F.col("end_date_dense_rank_2").asc_nulls_last())
w_end_date_rank_by_end_date_de_2 = Window.partitionBy("end_date_rank").orderBy(F.col("end_date_dense_rank_2").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_prev_end_date_year = Window.partitionBy("rn", F.year("prev_end_date"))
w_last_known_end_date_month_ne = Window.partitionBy(F.date_trunc("month", "last_known_end_date"), F.month("next2_end_date")).orderBy(F.col("end_date_dense_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_policys = (
    df_policies
    .filter(F.col("status").isin("ACTIVE", "CANCELLED", "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

with_details = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.dense_rank().over(w_holder_region_holder_id_by_e).alias("end_date_dense_rank"),
        F.row_number().over(w_holder_region_holder_id_by_e_2).alias("rn"),
        F.first("end_date", ignorenulls=True).over(w_end_date_month_product_by_en).alias("first_known_end_date"),
        F.lag("end_date").over(w_holder_region_holder_id_by_e_2).alias("prev_end_date"),
        F.rank().over(w_end_date_month_product_by_en_2).alias("end_date_rank"),
        F.dense_rank().over(w_end_date_month_product_by_en_3).alias("end_date_dense_rank_2"),
        F.round(F.percent_rank().over(w_all_by_end_date), 3).alias("end_date_percent_rank"),
        F.round(F.percent_rank().over(w_end_date_month_product_by_en_2), 3).alias("end_date_percent_rank_2"),
        F.round(F.percent_rank().over(w_holder_region_holder_id_by_e), 3).alias("end_date_percent_rank_3"),
        F.ntile(5).over(w_all_by_end_date).alias("end_date_quintile"),
        F.lead("end_date", 2).over(w_holder_region_holder_id_by_e_2).alias("next2_end_date"),
        F.last("end_date", ignorenulls=True).over(w_holder_region_holder_id_by_e_3).alias("last_known_end_date"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.round(F.cume_dist().over(w_end_date_rank_by_end_date_de), 3).alias("end_date_dense_rank_2_cume_dist"),
        F.last("end_date_rank", ignorenulls=True).over(w_end_date_rank_by_end_date_de_2).alias("last_known_end_date_rank"),
        F.sum("rn").over(w_rn_prev_end_date_year).alias("rn_prev_end_date_year_sum_rn"),
        F.last("prev_end_date").over(w_last_known_end_date_month_ne).alias("last_prev_end_date"),
    )
)

result = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "holder_segment",
        "holder_region",
        "first_known_end_date",
        "end_date_dense_rank_2",
        "end_date_percent_rank_3",
        "end_date_quintile",
        "last_known_end_date",
        "last_prev_end_date",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
        F.length(F.trim(F.col("holder_segment"))).alias("holder_segment_len"),
    )
    .orderBy(F.col("holder_segment").asc(), F.col("last_known_end_date").desc())
)
