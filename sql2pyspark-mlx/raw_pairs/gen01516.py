from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())

filtered = (
    df_shipments
    .filter(~F.col("status").isin(["RETURNED", "DELIVERED"]) & (F.col("delivered_date") > F.lit("2023-03-10").cast("date")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

with_windows = (
    filtered
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "status_prefix",
        F.ntile(5).over(w).alias("cost_bucket"),
        F.rank().over(w2).alias("cost_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("order_id").isNotNull() & F.col("cost_rank").between(5, 8))
    .select(
        "id",
        "order_id",
        "status_prefix",
        F.when(F.col("cost_rank") >= 7, "High").when(F.col("cost_rank") >= 4, "Medium").otherwise("Low").alias("cost_rank_level"),
        F.greatest("cost_bucket", "cost_rank").alias("greatest_cost_bucket_cost_rank"),
    )
)

remaining_keys = (
    base
    .select("id", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") <= "2024-04-01")
        .select("id", "order_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .rollup(
        F.when(F.col("greatest_cost_bucket_cost_rank") >= 89, "LARGE").otherwise("SMALL").alias("greatest_cost_bucket_cost_rank_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("greatest_cost_bucket_cost_rank", [0.25, 0.5, 0.75]).alias("greatest_cost_bucket_cost_rank_quartiles"),
    )
    .filter(F.col("num_rows") > 1)
)
