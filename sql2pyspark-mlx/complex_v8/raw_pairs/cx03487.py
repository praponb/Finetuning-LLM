from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product_by_start_date = Window.partitionBy("status", "product").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_product_by_start_date_2 = Window.partitionBy("status", "product").orderBy("start_date", "id")
w_status_product_by_start_date_3 = Window.partitionBy("status", "product").orderBy("start_date")
w_status_product_by_start_date_4 = Window.partitionBy("status", "product").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("product").orderBy(F.col("start_date_dense_rank").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").desc(), F.col("id").desc())

recent_policys = (
    df_policies
    .filter((F.col("status").isNotNull()) | (F.col("premium") >= 1042.5))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

with_windows = (
    recent_policys
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.nth_value("start_date", 2).over(w_status_product_by_start_date).alias("second_start_date"),
        F.rank().over(w_status_product_by_start_date_2).alias("start_date_rank"),
        F.round(F.cume_dist().over(w_status_product_by_start_date_3), 3).alias("start_date_cume_dist"),
        F.first("start_date", ignorenulls=True).over(w_status_product_by_start_date_4).alias("first_known_start_date"),
        F.dense_rank().over(w_status_product_by_start_date_3).alias("start_date_dense_rank"),
    )
)

deduped = (
    with_windows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows = (
    deduped
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "start_date",
        "status",
        "second_start_date",
        "start_date_rank",
        "start_date_cume_dist",
        "first_known_start_date",
        "start_date_dense_rank",
        F.datediff(F.col("second_start_date"), F.col("first_known_start_date")).alias("days_first_known_start_date_to_second_start_date"),
        F.round(F.col("start_date_cume_dist") * 0.25, 2).alias("start_date_cume_dist_adj"),
    )
    .orderBy(
        F.col("first_known_start_date").desc_nulls_first(),
        F.col("start_date_cume_dist_adj").desc_nulls_first(),
    )
)
