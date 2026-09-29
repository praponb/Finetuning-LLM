from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("stock_qty").desc(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("units") < 24)
    .select(
        "*",
        F.col("revenue").cast("int").alias("revenue_int"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.stock_qty"))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims = (
    deduped
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.city"), F.col("d.region"))
)

cleaned_store_sales = (
    with_dims
    .filter((F.col("sale_date") >= F.lit("2023-04-24").cast("date")) & F.col("city").isNotNull() & (F.col("units") == 26))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue_int",
        "sale_date_month_end",
        "stock_qty",
        "city",
        "region",
        (F.col("revenue_int") - F.col("stock_qty")).alias("revenue_int_minus_stock_qty"),
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
)

summary = (
    cleaned_store_sales
    .groupBy("region")
    .agg(
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.round(F.sum("greatest_revenue_units") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("greatest_revenue_units_per_tokyo"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
    )
)

result = (
    summary
    .select(
        "region",
        "new_york_events",
        "greatest_revenue_units_per_tokyo",
        "singapore_events",
    )
    .orderBy(F.col("region").desc_nulls_last(), "new_york_events")
    .limit(20)
)
