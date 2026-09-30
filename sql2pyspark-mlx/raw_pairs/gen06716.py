from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w3 = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_first())
w_ordered = Window.partitionBy("product_id", "brand").orderBy("quantity", "id")
w5 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("category").orderBy(F.col("price_percent_rank").asc_nulls_last())
w7 = Window.partitionBy("category").orderBy(F.col("cumulative_price").desc(), "id")
w8 = Window.partitionBy("order_id", "category").orderBy(F.col("cumulative_price").desc_nulls_first())
w9 = Window.partitionBy("order_id").orderBy("price_percent_rank", "id")
w10 = Window.partitionBy("category")
w_grp = Window.partitionBy("order_id").orderBy("unit_price_avg_6_prior_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("category").orderBy("cumulative_price_percent_rank", "id")

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "inner")
    .select("src.*", F.col("p.category"), F.col("p.brand"), F.col("p.price"))
)

enriched_order_items = (
    enriched
    .select(
        "id",
        "order_id",
        "unit_price",
        "category",
        "price",
        F.sum("price").over(w).alias("cumulative_price"),
        F.ntile(3).over(w2).alias("quantity_bucket"),
        F.percent_rank().over(w3).alias("price_percent_rank"),
        F.lag("price", 1).over(w_ordered).alias("prev_price"),
        F.avg("unit_price").over(w5).alias("unit_price_avg_6_prior"),
    )
)

scored = (
    enriched_order_items
    .select(
        "*",
        F.percent_rank().over(w6).alias("price_percent_rank_percent_rank"),
        F.row_number().over(w7).alias("event_seq"),
        F.percent_rank().over(w8).alias("cumulative_price_percent_rank"),
        F.lag("quantity_bucket", 1).over(w9).alias("prev_quantity_bucket"),
        F.round((F.col("unit_price_avg_6_prior") / F.sum("unit_price_avg_6_prior").over(w10)), 4).alias("unit_price_avg_6_prior_share"),
    )
)

scored_order_items = (
    scored
    .select(
        "id",
        "order_id",
        "category",
        "price",
        "cumulative_price",
        "quantity_bucket",
        "prev_price",
        "event_seq",
        "prev_quantity_bucket",
        "unit_price_avg_6_prior_share",
        F.min("price_percent_rank_percent_rank").over(w_grp).alias("running_min_price_percent_rank_percent_rank"),
        F.avg("price_percent_rank_percent_rank").over(w_grp).alias("running_avg_price_percent_rank_percent_rank"),
        F.sum("price_percent_rank_percent_rank").over(w_grp).alias("running_sum_price_percent_rank_percent_rank"),
        F.row_number().over(w12).alias("row_seq"),
    )
)

latest = (
    scored_order_items
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "category",
        "quantity_bucket",
        "prev_price",
        "unit_price_avg_6_prior_share",
        "running_min_price_percent_rank_percent_rank",
        "running_avg_price_percent_rank_percent_rank",
        "row_seq",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("running_min_price_percent_rank_percent_rank") >= 0.67, "LARGE").otherwise("SMALL").alias("running_min_price_percent_rank_percent_rank_tier"),
    )
    .agg(
        F.max(F.when(F.col("category") == "Fashion", F.col("running_min_price_percent_rank_percent_rank"))).alias("fashion_max_running_min_price_percent_rank_percent_rank"),
        F.count_if(F.col("category") == "Electronics").alias("electronics_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .orderBy(F.col("distinct_order_ids").asc_nulls_first())
)
