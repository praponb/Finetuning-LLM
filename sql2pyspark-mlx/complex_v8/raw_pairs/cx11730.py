from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product = Window.partitionBy("holder_id", "product")
w_product_by_end_date = Window.partitionBy("product").orderBy(F.col("end_date").asc_nulls_last(), "id")
w_all_by_premium_ties = Window.orderBy("premium")
w_product_by_end_date_moving_6 = Window.partitionBy("product").orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium = Window.orderBy("premium", "id")
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy("premium", "id")

scoped_policys = (
    df_policies
    .filter(F.col("premium") <= 3817.5)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

sequenced = (
    scoped_policys
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_product), F.lit(0)), 4).alias("premium_share_of_holder_id_product"),
        F.round(F.cume_dist().over(w_product_by_end_date), 3).alias("end_date_cume_dist"),
        F.dense_rank().over(w_all_by_premium_ties).alias("premium_dense_rank"),
        F.min("premium").over(w_holder_id_product).alias("holder_id_product_min_premium"),
        F.round(F.avg("premium").over(w_product_by_end_date_moving_6), 2).alias("premium_avg_7"),
        F.sum("premium").over(w_all_by_premium_running).alias("running_premium"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_all_by_premium)).alias("days_since_prev_end_date"),
        F.round(F.avg("premium").over(w_product_holder_id), 2).alias("product_holder_id_avg_premium"),
        F.round(F.avg("premium").over(w_holder_id_product), 2).alias("holder_id_product_avg_premium"),
        F.lag("premium", 1, 0).over(w_holder_id_product_by_premium).alias("prev_premium"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

grouped = (
    derived
    .groupBy("premium_dense_rank", "holder_id", "product")
    .agg(
        F.sum("days_since_end_date").alias("total_days_since_end_date"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.countDistinct("product").alias("distinct_products"),
        F.count("*").alias("n_rows"),
        F.max("end_date").alias("last_end_date"),
        F.min("premium_dense_rank").alias("min_premium_dense_rank"),
    )
)

result = (
    grouped
    .filter(F.col("distinct_products") >= 3)
    .select(
        "premium_dense_rank",
        "holder_id",
        "product",
        "total_days_since_end_date",
        "distinct_holder_ids",
        "distinct_products",
        "n_rows",
        "last_end_date",
        "min_premium_dense_rank",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
    .orderBy(
        F.col("min_premium_dense_rank").desc_nulls_first(),
        F.col("last_end_date").desc(),
    )
)
