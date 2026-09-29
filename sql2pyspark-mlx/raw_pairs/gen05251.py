from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category", "store_id")
w4 = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w_part = Window.partitionBy("product_id").orderBy("cumulative_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("seq_no", "id")
w7 = Window.orderBy("prev_price", "id").rowsBetween(-2, -1)
w8 = Window.orderBy(F.col("running_count_seq_no").desc_nulls_last())
w9 = Window.orderBy("running_sum_seq_no", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.orderBy(F.col("running_count_seq_no").asc_nulls_last())

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-04-17").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        (F.datediff(F.current_date(), "sale_date") / 30).alias("months_since_sale_date"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.id", "e.store_id", "e.product_id", F.col("b.price"), F.col("b.category"))
)

enriched = (
    joined
    .select(
        "*",
        F.first("category", ignorenulls=True).over(w).alias("first_known_category"),
        F.sum("price").over(w2).alias("running_price"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("price").over(w).alias("cumulative_price"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "price",
        "seq_no",
        F.max("seq_no").over(w_part).alias("running_max_seq_no"),
        F.sum("seq_no").over(w_part).alias("running_sum_seq_no"),
        F.count("seq_no").over(w_part).alias("running_count_seq_no"),
        F.lag("price", 1).over(w_grp).alias("prev_price"),
    )
)

valid = (
    metrics
    .filter(F.col("running_sum_seq_no") < 39)
    .withColumn("price_plus_running_sum_seq_no", (F.col("price") + F.col("running_sum_seq_no")))
)

metrics_store_sales = (
    valid
    .withColumn("price_avg_2_prior", F.avg("price").over(w7))
    .withColumn("running_count_seq_no_dense_rank", F.dense_rank().over(w8))
    .withColumn("running_sum_seq_no_avg_3", F.avg("running_sum_seq_no").over(w9))
    .withColumn("running_count_seq_no_percent_rank", F.percent_rank().over(w10))
)

result = (
    metrics_store_sales
    .groupBy(
        F.when(F.col("prev_price") >= 1290, "PREMIUM").otherwise("STANDARD").alias("prev_price_tier"),
    )
    .agg(
        F.percentile_approx("running_count_seq_no", [0.25, 0.5, 0.75]).alias("running_count_seq_no_quartiles"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count")
)
