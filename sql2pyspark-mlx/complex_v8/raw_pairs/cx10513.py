from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product_by_start_date = Window.partitionBy("status", "product").orderBy(F.col("start_date").desc(), "id")
w_status_product_by_start_date_2 = Window.partitionBy("status", "product").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_seq_no_running = Window.orderBy(F.col("seq_no").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_holder_id = Window.partitionBy("status", "holder_id")

recent_policys = (
    df_policies
    .filter(~F.col("product").isin("HEALTH", "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

sequenced = (
    recent_policys
    .select(
        "*",
        F.row_number().over(w_status_product_by_start_date).alias("seq_no"),
        F.first("start_date").over(w_status_product_by_start_date_2).alias("first_start_date"),
        F.round(F.cume_dist().over(w_status_product_by_start_date), 3).alias("start_date_cume_dist"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_product_by_start_date)).alias("days_since_prev_start_date"),
    )
)

ranked = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "seq_no",
        "first_start_date",
        "start_date_cume_dist",
        F.last("start_date", ignorenulls=True).over(w_all_by_seq_no_running).alias("last_known_start_date"),
        F.sum("days_since_prev_start_date").over(w_status_holder_id).alias("status_holder_id_sum_days_since_prev_start_date"),
    )
)

result = (
    ranked
    .rollup("status", "seq_no", "product")
    .agg(
        F.count("*").alias("cnt"),
        F.min("start_date_cume_dist").alias("min_start_date_cume_dist"),
        F.max("start_date").alias("last_start_date"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "seq_no",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "cnt",
        "min_start_date_cume_dist",
        "last_start_date",
    )
)
