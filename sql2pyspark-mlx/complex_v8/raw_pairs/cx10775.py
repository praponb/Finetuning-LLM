from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_by_start_da = Window.partitionBy("holder_id", "status").orderBy("start_date", "id")
w_start_date_year = Window.partitionBy(F.year("start_date"))

scoped = (
    df_policies
    .filter(F.col("holder_id") <= 15)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.dense_rank().over(w_holder_id_status_by_start_da).alias("start_date_dense_rank"),
        F.max("premium").over(w_start_date_year).alias("start_date_year_max_premium"),
        F.row_number().over(w_holder_id_status_by_start_da).alias("seq_no"),
    )
)

stats = (
    with_trends
    .groupBy("status", "seq_no")
    .agg(
        F.sum("seq_no").alias("total_seq_no"),
        F.datediff(F.max("start_date"), F.min("start_date")).alias("start_date_span_days"),
        F.round(F.avg("start_date_year_max_premium"), 2).alias("avg_start_date_year_max_premium"),
        F.count("*").alias("n_rows"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    stats
    .select(
        "status",
        "seq_no",
        "total_seq_no",
        "avg_start_date_year_max_premium",
        "n_rows",
        (F.col("distinct_status") + F.col("n_rows")).alias("distinct_status_plus_n_rows"),
    )
    .orderBy(
        F.col("avg_start_date_year_max_premium").desc_nulls_first(),
        F.col("total_seq_no").desc(),
        "status",
        "seq_no",
    )
    .limit(10)
)
