from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy(F.col("end_date").desc(), F.col("id").desc())
w_product_by_premium_ties = Window.partitionBy("product").orderBy("premium")
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_product_by_premium_full = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id = Window.partitionBy("product", "holder_id")

filtered = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-01-24").cast("date"))
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

unique_rows = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    unique_rows
    .select(
        "*",
        F.dense_rank().over(w_product_by_premium_ties).alias("premium_dense_rank"),
        F.row_number().over(w_product_by_premium).alias("seq_no"),
        F.nth_value("start_date", 2).over(w_product_by_premium_full).alias("second_start_date"),
        F.max("premium").over(w_product_holder_id).alias("product_holder_id_max_premium"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "premium_dense_rank",
        "seq_no",
        "second_start_date",
        "product_holder_id_max_premium",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
    .orderBy("id")
)
