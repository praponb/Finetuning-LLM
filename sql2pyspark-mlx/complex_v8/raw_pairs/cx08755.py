from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_len_product = Window.partitionBy("status_len", "product")
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")

recent_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

with_windows = (
    recent_policys
    .select(
        "id",
        "start_date",
        "end_date",
        "status_len",
        F.round(F.col("status_len") / F.nullif(F.sum("status_len").over(w_status_len_product), F.lit(0)), 4).alias("status_len_share_of_status_len_product"),
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.rank().over(w_all_by_start_date).alias("start_date_rank"),
        F.min("status_len").over(w_status_len_product).alias("status_len_product_min_status_len"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("status_len").alias("m_status_len"),
        F.col("start_date_rank").alias("start_date_rank_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_status_len", "start_date_rank_v"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
