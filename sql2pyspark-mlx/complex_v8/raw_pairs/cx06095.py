from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_len = Window.partitionBy("holder_id", "status_len")
w_status_len_by_status_len = Window.partitionBy("status_len").orderBy(F.col("status_len").desc(), "id")
w_holder_id_product_by_status = Window.partitionBy("holder_id", "product").orderBy(F.col("status_len").desc(), "id")
w_status_product = Window.partitionBy("status", "product")
w_status_len_by_status_len_pri = Window.partitionBy("status_len").orderBy(F.col("status_len").desc(), "id").rowsBetween(-1, -1)
w_distinct_product_in_holder_i = Window.partitionBy("distinct_product_in_holder_id_status_len").orderBy(F.col("start_date").desc(), "id")
w_status_len_distinct_holder_i = Window.partitionBy("status_len", "distinct_holder_id_in_status_product").orderBy("status_product_max_status_len", "id")
w_status_len_distinct_product = Window.partitionBy("status_len", "distinct_product_in_holder_id_status_len").orderBy(F.col("distinct_product_in_holder_id_status_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_len_distinct_holder_i_2 = Window.partitionBy("status_len", "distinct_holder_id_in_status_product").orderBy("status_product_max_status_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_holder_id_in_status = Window.partitionBy("distinct_holder_id_in_status_product").orderBy("start_date", "id")

recent = (
    df_policies
    .filter(F.col("premium") < 4678.8)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

with_windows = (
    recent
    .select(
        "id",
        "start_date",
        "status_len",
        F.count(F.lit(1)).over(w_holder_id_status_len).alias("rows_in_holder_id_status_len"),
        F.lag("end_date", 3).over(w_status_len_by_status_len).alias("prev3_end_date"),
        F.round(F.percent_rank().over(w_holder_id_product_by_status), 3).alias("status_len_percent_rank"),
        F.max("status_len").over(w_status_product).alias("status_product_max_status_len"),
        F.max("status_len").over(w_status_len_by_status_len_pri).alias("status_len_max_1_prior"),
        F.size(F.collect_set("holder_id").over(w_status_product)).alias("distinct_holder_id_in_status_product"),
        F.round(F.col("status_len") - F.avg("status_len").over(w_holder_id_status_len), 2).alias("status_len_vs_holder_id_status_len_avg"),
        F.size(F.collect_set("product").over(w_holder_id_status_len)).alias("distinct_product_in_holder_id_status_len"),
    )
)

annotated = (
    with_windows
    .select(
        "*",
        F.datediff(F.col("start_date"), F.col("prev3_end_date")).alias("days_prev3_end_date_to_start_date"),
        F.round(F.col("status_len_max_1_prior") / F.nullif(F.col("status_len"), F.lit(0)), 3).alias("status_len_max_1_prior_per_status_len"),
    )
)

windowed = (
    annotated
    .select(
        "id",
        "start_date",
        "rows_in_holder_id_status_len",
        "prev3_end_date",
        "status_len_percent_rank",
        "status_product_max_status_len",
        F.round(F.percent_rank().over(w_distinct_product_in_holder_i), 3).alias("start_date_percent_rank"),
        F.round(F.percent_rank().over(w_status_len_distinct_holder_i), 3).alias("status_product_max_status_len_percent_rank"),
        F.nth_value("status_len_percent_rank", 2).over(w_status_len_distinct_product).alias("second_status_len_percent_rank"),
        F.last("status_len").over(w_status_len_distinct_holder_i_2).alias("last_status_len"),
        F.lead("status_len", 3).over(w_distinct_holder_id_in_status).alias("next3_status_len"),
    )
)

result = (
    windowed
    .select(
        "id",
        "start_date",
        "prev3_end_date",
        "status_len_percent_rank",
        "status_product_max_status_len",
        "start_date_percent_rank",
        "status_product_max_status_len_percent_rank",
        "second_status_len_percent_rank",
        "last_status_len",
        "next3_status_len",
        F.coalesce(F.col("status_len_percent_rank"), F.lit(0.0)).alias("status_len_percent_rank_filled"),
    )
    .orderBy(
        F.col("second_status_len_percent_rank").asc_nulls_last(),
        F.col("next3_status_len").desc(),
    )
)
