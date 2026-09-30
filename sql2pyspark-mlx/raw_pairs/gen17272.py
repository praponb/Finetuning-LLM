from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("product_id", "id", "line_no").rowsBetween(-3, -1)
w_grp = Window.orderBy("product_id", "id", "line_no").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("customer_id")
w4 = Window.orderBy("line_no", "id", "line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_ordered = Window.orderBy("product_id_avg_3_prior", "id", "line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("line_no").desc(), "id", "line_no")
w8 = Window.orderBy(F.col("product_id_avg_3_prior").desc())
w9 = Window.partitionBy(F.month("created_at")).orderBy("customer_id_min_line_no", "id", "line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("created_at", "id", "line_no").rowsBetween(-3, Window.currentRow)

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("line_no"),
        F.col("line.product_id"),
        F.col("line.qty"),
    )
)

with_windows = (
    flattened
    .select(
        "id",
        "created_at",
        "line_no",
        "product_id",
        "qty",
        F.avg("product_id").over(w).alias("product_id_avg_3_prior"),
        F.avg("line_no").over(w_grp).alias("line_no_avg_2"),
        F.min("line_no").over(w_part).alias("customer_id_min_line_no"),
        F.count("qty").over(w_part).alias("customer_id_count_qty"),
    )
)

valid = (
    with_windows
    .filter(F.col("created_at") > F.lit("2025-03-01 12:00:00").cast("timestamp"))
    .select(
        "id",
        "created_at",
        "line_no",
        "product_id",
        "qty",
        "product_id_avg_3_prior",
        "line_no_avg_2",
        "customer_id_min_line_no",
        "customer_id_count_qty",
        (F.col("line_no") + F.col("product_id")).alias("line_no_plus_product_id"),
        F.col("product_id").cast("string").alias("product_id_num"),
    )
)

enriched = (
    valid
    .withColumn("cumulative_qty", F.sum("qty").over(w4))
    .withColumn("line_no_sum_2h", F.sum("line_no").over(w5))
    .withColumn("running_qty", F.sum("qty").over(w_ordered))
)

enriched_carts = (
    enriched
    .filter(F.col("customer_id_count_qty") < 15)
    .select(
        "*",
        F.row_number().over(w7).alias("row_seq"),
        F.dense_rank().over(w8).alias("product_id_avg_3_prior_dense_rank"),
        F.sum("cumulative_qty").over(w9).alias("running_cumulative_qty"),
        F.avg("product_id").over(w10).alias("product_id_avg_3"),
    )
)

result = (
    enriched_carts
    .select(
        "id",
        "created_at",
        "line_no",
        "product_id",
        "qty",
        "line_no_avg_2",
        "customer_id_min_line_no",
        "customer_id_count_qty",
        "line_no_plus_product_id",
    )
    .orderBy("line_no_avg_2")
)
