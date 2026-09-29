from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())
w_hist = Window.orderBy("revenue", "id")

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-06-27").cast("date")) & F.col("product_id").isin([1, 3, 8, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.when(F.col("units") >= 38, "High").when(F.col("units") >= 12, "Medium").otherwise("Low").alias("units_tier"),
        F.date_sub("sale_date", 45).alias("sale_date_minus_45d"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.units",
        "src.revenue",
        "src.units_tier",
        F.col("b.sqft"),
        F.col("b.city"),
        F.col("b.region"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "store_id",
        "revenue",
        "units_tier",
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.lag("units").over(w).alias("prev_units"),
        F.lead("units").over(w).alias("next_units"),
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("sqft_dense_rank"),
    )
)

base = (
    enriched
    .filter(F.col("seq_no").between(5, 6))
    .select(
        "id",
        "revenue",
        "units_tier",
        "prev_revenue",
        "next_revenue",
        "prev_units",
        "next_units",
        "seq_no",
        "sqft_dense_rank",
        F.greatest("next_revenue", "revenue").alias("greatest_next_revenue_revenue"),
    )
)

with_windows = (
    base
    .withColumn("prev_sqft_dense_rank", F.lag("sqft_dense_rank").over(w_hist))
    .withColumn("next_sqft_dense_rank", F.lead("sqft_dense_rank").over(w_hist))
    .withColumn("prev_next_units", F.lag("next_units").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_units") >= 44, "LARGE").otherwise("SMALL").alias("prev_units_tier"),
    )
    .agg(
        F.round(F.avg("greatest_next_revenue_revenue"), 2).alias("avg_greatest_next_revenue_revenue"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.percentile_approx("prev_revenue", [0.25, 0.5, 0.75]).alias("prev_revenue_quartiles"),
    )
)
