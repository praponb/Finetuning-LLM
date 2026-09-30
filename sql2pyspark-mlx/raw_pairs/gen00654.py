from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("price", "id")
w3 = Window.partitionBy("brand", "category")
w4 = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("category")
w6 = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(-3, -1)
w7 = Window.orderBy(F.col("price_bucket_avg_3_prior").desc_nulls_first())
w8 = Window.partitionBy(F.date_trunc("MONTH", "launch_date")).orderBy("cumulative_cost", "id")
w9 = Window.orderBy("category_min_cost_avg_5", "id")

valid = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-05-11").cast("date"))
    .withColumn("is_sale", F.array_contains("tags", "sale"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

enriched = (
    valid
    .select(
        "id",
        "category",
        "cost",
        "launch_date",
        F.avg("cost").over(w).alias("cost_avg_5"),
        F.ntile(5).over(w2).alias("price_bucket"),
        F.sum("price").over(w3).alias("brand_sum_price"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "cost",
        "launch_date",
        "price_bucket",
        F.sum("cost").over(w4).alias("cumulative_cost"),
        F.min("cost_avg_5").over(w5).alias("category_min_cost_avg_5"),
        F.avg("price_bucket").over(w6).alias("price_bucket_avg_3_prior"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") == 3), "left_semi")
)

kept_products = (
    kept
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([6, 17, 18, 31])), "left_semi")
)

metrics = (
    kept_products
    .select(
        "*",
        F.rank().over(w7).alias("price_bucket_avg_3_prior_rank"),
        F.lag("cumulative_cost", 2).over(w8).alias("prev_cumulative_cost"),
        F.lead("cumulative_cost", 1).over(w9).alias("next_cumulative_cost"),
    )
)

totals = (
    metrics
    .cube(
        F.date_trunc("month", "launch_date").alias("activity_month"),
        F.date_trunc("year", "launch_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("category_min_cost_avg_5", [0.25, 0.5, 0.75]).alias("category_min_cost_avg_5_quartiles"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.percentile_approx("cumulative_cost", [0.25, 0.5, 0.75]).alias("cumulative_cost_quartiles"),
    )
)

result = (
    totals
    .select(
        "activity_month",
        "year_start",
        "category_min_cost_avg_5_quartiles",
        "cost_quartiles",
    )
    .orderBy(F.col("year_start").desc())
)
