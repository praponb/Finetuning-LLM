from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("running_avg_price_rounded").asc_nulls_last())
w3 = Window.partitionBy(F.year("launch_date"))
w_running = Window.partitionBy(F.date_trunc("YEAR", "launch_date")).orderBy(F.col("running_avg_price_rounded").asc_nulls_last())

valid = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-06-26"))) & (F.col("stock_qty") >= 107))
    .select(
        "id",
        "product_name",
        "price",
        "launch_date",
        "tags",
        F.round("price", 2).alias("price_rounded"),
    )
)

base = (
    valid
    .filter(F.col("price") < 1100)
    .select(
        "id",
        "price",
        "launch_date",
        "tags",
        "price_rounded",
        F.lower("product_name").alias("product_name_lower"),
        F.date_format("launch_date", "MMM yyyy").alias("launch_date_label"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.sum("price_rounded").over(w_grp).alias("running_sum_price_rounded"),
        F.avg("price_rounded").over(w_grp).alias("running_avg_price_rounded"),
        F.min("price_rounded").over(w_grp).alias("running_min_price_rounded"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .intersect(
        df_products
        .filter(F.size("tags") >= 1)
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w2).alias("running_avg_price_rounded_dense_rank"),
        F.min("running_sum_price_rounded").over(w3).alias("launch_date_year_min_running_sum_price_rounded"),
        F.dense_rank().over(w_running).alias("running_avg_price_rounded_dense_rank_2"),
    )
)

result = (
    scored
    .select(
        "launch_date",
        "tags",
        "price_rounded",
        "product_name_lower",
        "launch_date_label",
        "running_sum_price_rounded",
        "running_avg_price_rounded",
        "running_min_price_rounded",
        "running_avg_price_rounded_dense_rank",
    )
    .orderBy(F.col("price_rounded").desc())
)
