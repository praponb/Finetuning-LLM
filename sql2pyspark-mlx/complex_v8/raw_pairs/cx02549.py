from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.month("end_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_end_date_prior_1 = Window.partitionBy("product").orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy("premium_distance", "id")

scoped_policys = (
    df_policies
    .filter((F.col("premium") < 3817.5) & (F.col("holder_id") >= 18))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

scored = (
    scoped_policys
    .filter(~F.col("product").isin("AUTO", "TRAVEL"))
    .select("*", F.abs(F.col("premium") - 2096.6).alias("premium_distance"))
)

metrics = (
    scored
    .select(
        "*",
        F.round(F.col("premium_filled") - F.avg("premium_filled").over(w_holder_id), 2).alias("premium_filled_vs_holder_id_avg"),
        F.max("premium_filled").over(w_holder_id_end_date_month_by).alias("max_premium_filled_so_far"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.sum("premium_distance").over(w_product_by_end_date_prior_1).alias("premium_distance_sum_1_prior"),
    )
)

sequenced = (
    metrics
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        "premium_distance",
        "premium_filled_vs_holder_id_avg",
        "max_premium_filled_so_far",
        "rows_in_holder_id",
        "premium_vs_holder_id_avg",
        "premium_distance_sum_1_prior",
        F.round(F.col("rows_in_holder_id") - F.avg("rows_in_holder_id").over(w_product_holder_id), 2).alias("rows_in_holder_id_vs_product_holder_id_avg"),
        F.lag("premium").over(w_holder_id_product_by_premium).alias("prev_premium"),
        F.min("premium_distance").over(w_product_holder_id).alias("product_holder_id_min_premium_distance"),
    )
)

result = (
    sequenced
    .groupBy("end_date")
    .pivot("product", ["LIFE", "HOME", "TRAVEL", "AUTO"])
    .agg(F.count("premium_vs_holder_id_avg"))
    .orderBy("end_date")
)
