from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id", "store_id")
w2 = Window.orderBy("sale_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("product_id").orderBy(F.col("stock_qty").desc(), "id")
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("revenue").alias("min_store_sale_revenue"))

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-03-25").cast("date")) & (F.col("sale_date") < "2023-09-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "product_id",
        F.count("units").over(w_hist).alias("product_id_count_units"),
        F.avg("revenue").over(w2).alias("revenue_avg_5"),
        F.avg("units").over(w3).alias("units_avg_5_prior"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.units_avg_5_prior",
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    ranked
    .select("id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") <= "2023-04-01")
        .select("id", "product_id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("product_id").isin([2, 3, 4, 8]))
    .select(
        "product_id",
        "units_avg_5_prior",
        "stock_qty",
        "dedup_rank",
        F.round((F.col("dedup_rank") * 0.9), 2).alias("dedup_rank_adjusted"),
        F.date_add("launch_date", 15).alias("launch_date_plus_15d"),
    )
)

result = (
    base
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "stock_qty",
        "dedup_rank",
        "dedup_rank_adjusted",
        "launch_date_plus_15d",
        F.col("min_store_sale_revenue"),
    )
)
