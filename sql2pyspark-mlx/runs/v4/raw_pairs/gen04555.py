from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc_nulls_last())

latest = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .withColumn("quantity_bucket", F.ntile(4).over(w2))
    .withColumn("quantity_avg_6", F.avg("quantity").over(w3))
    .withColumn("quantity_percent_rank", F.percent_rank().over(w4))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.product_name"), F.col("p.price"), F.col("p.launch_date"))
)

joined = (
    with_dims
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.category"),
        F.col("p.cost"),
        F.col("p.price").alias("product_price"),
    )
)

summary = (
    joined
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
        "product_id",
    )
    .agg(
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("price_per_grocery"),
        F.round(F.sum("quantity_bucket") / F.nullif(F.count(F.when(F.col("category") == "Sports", 1)), F.lit(0)), 2).alias("quantity_bucket_per_sports"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.percentile_approx("quantity_bucket", [0.25, 0.5, 0.75]).alias("quantity_bucket_quartiles"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") < 1)
)

result = (
    summary
    .select(
        "week_start",
        "activity_month",
        "product_id",
        "price_per_grocery",
        "quantity_bucket_per_sports",
        "num_categories",
        "quantity_bucket_quartiles",
        "num_rows",
    )
    .orderBy(F.col("num_categories").asc_nulls_last())
)
