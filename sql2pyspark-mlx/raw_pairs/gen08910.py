from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy(F.year("launch_date"), "category").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("week", "launch_date"), "category").orderBy(F.col("quantity").asc_nulls_first())
w4 = Window.partitionBy("category", "store_id")
w5 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w6 = Window.orderBy("category_sum_quantity", "id").rowsBetween(-3, -1)
w_part = Window.orderBy(F.col("sqft_percent_rank").desc(), "id")

joined = (
    df_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.quantity",
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("p.price"),
    )
)

enriched = (
    joined
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.opened_date") <= "2023-08-01")
    .select(
        "a.id",
        "a.store_id",
        "a.quantity",
        "a.launch_date",
        "a.category",
        F.col("b.sqft"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "store_id",
        "quantity",
        "launch_date",
        F.percent_rank().over(w).alias("sqft_percent_rank"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.percent_rank().over(w3).alias("quantity_percent_rank"),
        F.sum("quantity").over(w4).alias("category_sum_quantity"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "quantity",
        "sqft_percent_rank",
        "quantity_percent_rank",
        "category_sum_quantity",
    )
)

valid = (
    latest
    .filter(F.col("sqft_percent_rank") >= 0.73)
    .withColumn("sqft_percent_rank_tier", F.when(F.col("sqft_percent_rank") >= 0.76, "High").when(F.col("sqft_percent_rank") >= 0.19, "Medium").otherwise("Low"))
    .withColumn("category_sum_quantity_band", F.when(F.col("category_sum_quantity") >= 1114, "High").when(F.col("category_sum_quantity") >= 249, "Medium").otherwise("Low"))
)

with_windows = (
    valid
    .select(
        "id",
        "quantity",
        "quantity_percent_rank",
        "sqft_percent_rank_tier",
        "category_sum_quantity_band",
        F.avg("sqft_percent_rank").over(w6).alias("sqft_percent_rank_avg_3_prior"),
        F.ntile(5).over(w_part).alias("sqft_percent_rank_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("quantity") >= 218, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("quantity_tier")
)
