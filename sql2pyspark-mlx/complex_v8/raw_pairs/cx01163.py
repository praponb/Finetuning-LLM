from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_end_date = Window.partitionBy("product").orderBy(F.col("end_date").desc(), "id")
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_start_date_month_holder_id = Window.partitionBy(F.date_trunc("month", "start_date"), "holder_id")
w_holder_id_start_date_month = Window.partitionBy("holder_id", F.month("start_date"))
w_product = Window.partitionBy("product")
w_product_by_end_date_prior_3 = Window.partitionBy("product").orderBy(F.col("end_date").desc(), "id").rowsBetween(-3, -1)
w_product_by_end_date_full = Window.partitionBy("product").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_products = Window.partitionBy("distinct_products")
w_end_date_quartile_distinct_p = Window.partitionBy("end_date_quartile", "distinct_products").orderBy("last_end_date", "rn", "end_date_quartile", "holder_id").rowsBetween(-6, Window.currentRow)

prepared = (
    df_policies
    .filter((~F.col("status").isin("CANCELLED", "EXPIRED")) & (F.col("premium") >= 1051.9))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.ntile(4).over(w_product_by_end_date).alias("end_date_quartile"),
        F.lag("end_date").over(w_product_by_premium).alias("prev_end_date"),
        F.round(F.avg("premium").over(w_start_date_month_holder_id), 2).alias("start_date_month_holder_id_avg_premium"),
        F.count(F.lit(1)).over(w_holder_id_start_date_month).alias("rows_in_holder_id_start_date_month"),
        F.row_number().over(w_product_by_premium).alias("rn"),
        F.max("premium").over(w_product).alias("product_max_premium"),
        F.max("premium").over(w_product_by_end_date_prior_3).alias("premium_max_3_prior"),
        F.first("end_date").over(w_product_by_end_date_full).alias("first_end_date"),
    )
)

totals = (
    ranked
    .groupBy("rn", "end_date_quartile", "holder_id")
    .agg(
        F.sum("premium").alias("total_premium"),
        F.max("end_date").alias("last_end_date"),
        F.countDistinct("product").alias("distinct_products"),
        F.round(F.sum("start_date_month_holder_id_avg_premium") / F.nullif(F.sum("rn"), F.lit(0)), 3).alias("start_date_month_holder_id_avg_premium_to_rn_rat"),
        F.avg("rn").alias("avg_rn_check"),
    )
    .filter(F.col("avg_rn_check") > 4)
    .drop("avg_rn_check")
)

with_trends = (
    totals
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_distinct_products)).alias("distinct_holder_id_in_distinct_products"),
        F.sum("rn").over(w_end_date_quartile_distinct_p).alias("rn_sum_7"),
    )
)

result = (
    with_trends
    .select(
        "rn",
        "holder_id",
        "total_premium",
        "last_end_date",
        "start_date_month_holder_id_avg_premium_to_rn_rat",
        "distinct_holder_id_in_distinct_products",
        F.greatest(F.col("end_date_quartile"), F.col("rn")).alias("greatest_end_date_quartile_rn"),
        F.coalesce(F.col("end_date_quartile"), F.lit(0)).alias("end_date_quartile_filled"),
    )
    .orderBy(F.col("greatest_end_date_quartile_rn").asc_nulls_last())
)
