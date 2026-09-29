from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_end_date = Window.partitionBy("product").orderBy("end_date", "id")
w_end_date_month_by_end_date = Window.partitionBy(F.month("end_date")).orderBy("end_date", "id")
w_all_by_end_date_ties = Window.orderBy("end_date")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_product_by_end_date_full = Window.partitionBy("product").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy(F.col("end_date").asc_nulls_last())
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-11-20").cast("date")) & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

metrics = (
    base
    .select(
        "*",
        F.ntile(10).over(w_product_by_end_date).alias("end_date_decile"),
        F.dense_rank().over(w_end_date_month_by_end_date).alias("end_date_dense_rank"),
        F.dense_rank().over(w_all_by_end_date_ties).alias("end_date_dense_rank_2"),
        F.row_number().over(w_all_by_end_date).alias("seq_no"),
        F.first("end_date").over(w_product_by_end_date_full).alias("first_end_date"),
        F.row_number().over(w_end_date_month_by_end_date).alias("rn"),
        F.rank().over(w_holder_id_by_end_date_ties).alias("end_date_rank"),
        F.last("end_date", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_end_date"),
    )
)

labeled = (
    metrics
    .select(
        "*",
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
        F.round(F.col("seq_no") / F.nullif(F.col("end_date_decile"), F.lit(0)), 3).alias("seq_no_per_end_date_decile"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

result = (
    labeled
    .select(
        "id",
        "holder_id",
        "status",
        "end_date_decile",
        "end_date_dense_rank",
        "end_date_dense_rank_2",
        "seq_no",
        "rn",
        "last_known_end_date",
        "product_clean",
        "seq_no_per_end_date_decile",
        "status_slug",
        F.abs(F.col("end_date_rank") - 7).alias("end_date_rank_distance"),
    )
    .orderBy(
        F.col("end_date_dense_rank").asc_nulls_last(),
        F.col("end_date_rank_distance").asc_nulls_last(),
    )
)
