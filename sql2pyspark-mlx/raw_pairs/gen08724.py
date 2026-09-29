from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("brand").orderBy("price", "id")
w3 = Window.partitionBy("category").orderBy(F.col("stock_qty").asc_nulls_first())
w4 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w5 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("category").orderBy("next_price", "id").rowsBetween(-6, -1)

cleaned = (
    df_products
    .filter(F.col("stock_qty") == 398)
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.date_trunc("year", "launch_date").alias("launch_date_period"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"), F.col("c.quantity"))
)

scored = (
    latest_match
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "resolved_brand",
        "unit_price",
        F.lead("price", 1).over(w_recent).alias("next_price"),
        F.percent_rank().over(w3).alias("stock_qty_percent_rank"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "category",
        "price",
        "cost",
        "launch_date",
        "resolved_brand",
        "next_price",
        "stock_qty_percent_rank",
        F.sum("stock_qty_percent_rank").over(w5).alias("cumulative_stock_qty_percent_rank"),
        F.avg("next_price").over(w6).alias("next_price_avg_6_prior"),
    )
)

matched_keys = (
    metrics
    .select("category", "id")
    .exceptAll(
        df_products
        .filter(F.col("category") == "Books")
        .select("category", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("cost").between(420, 650))
    .select("*", F.date_sub("launch_date", 72).alias("launch_date_minus_72d"))
)

summary = (
    base
    .groupBy("category")
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.round(F.avg("next_price_avg_6_prior"), 2).alias("avg_next_price_avg_6_prior"),
        F.sum("price").alias("total_price"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("stock_qty_percent_rank").alias("max_stock_qty_percent_rank"),
    )
)

result = (
    summary
    .select(
        "category",
        "num_categories",
        "total_price",
        "sports_rows",
        "max_stock_qty_percent_rank",
    )
)
