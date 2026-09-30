from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id", "promo").orderBy("price", "id", "position").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "created_at"), "customer_id").orderBy("position", "id", "position")
w3 = Window.partitionBy(F.date_trunc("YEAR", "created_at"), "promo").orderBy("running_sum_product_id", "id", "position").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("product_id", "id", "position")
w5 = Window.partitionBy("promo")
w6 = Window.partitionBy("promo").orderBy("product_id", "id", "position")

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("elem.product_id"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.avg("product_id").over(w_ordered).alias("running_avg_product_id"),
        F.min("product_id").over(w_ordered).alias("running_min_product_id"),
        F.count("product_id").over(w_ordered).alias("running_count_product_id"),
        F.sum("product_id").over(w_ordered).alias("running_sum_product_id"),
        F.lag("product_id", 1).over(w2).alias("prev_product_id"),
    )
)

enriched = (
    metrics
    .filter(F.col("created_at") >= "2024-03-31")
    .select(
        "id",
        "position",
        "product_id",
        "running_min_product_id",
        "running_count_product_id",
        "running_sum_product_id",
        F.sum("position").over(w3).alias("cumulative_position"),
        F.lag("running_count_product_id", 1).over(w4).alias("prev_running_count_product_id"),
        F.avg("price").over(w5).alias("promo_avg_price"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

filtered = (
    enriched
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "position",
        "running_min_product_id",
        "running_count_product_id",
        "running_sum_product_id",
        "cumulative_position",
        "row_seq",
        F.ceil("promo_avg_price").alias("promo_avg_price_whole"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("created_at") >= "2024-02-02")
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("running_sum_product_id_tier", F.when(F.col("running_sum_product_id") >= 40, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_sum_product_id_tier", "running_count_product_id"], ["running_sum_product_id_tier"], []],
        "running_sum_product_id_tier",
        "running_count_product_id",
    )
    .agg(
        F.max("running_count_product_id").alias("max_running_count_product_id"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.percentile_approx("running_count_product_id", 0.95).alias("running_count_product_id_p95"),
        F.sum("running_sum_product_id").alias("total_running_sum_product_id"),
    )
    .filter(F.col("row_count_2") > 1)
    .orderBy(F.col("row_count_2").desc())
)
