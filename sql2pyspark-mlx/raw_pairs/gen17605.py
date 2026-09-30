from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city").orderBy("sale_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-06-10").cast("date")) & F.col("product_id").isin([1, 3, 4, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.sqft"), F.col("b.city"), F.col("b.region"))
)

cleaned_store_sales = (
    enriched
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-02-13"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

enriched_store_sales = (
    cleaned_store_sales
    .withColumn("running_sum_sqft", F.sum("sqft").over(w_recent))
    .withColumn("running_avg_sqft", F.avg("sqft").over(w_recent))
    .withColumn("running_max_sqft", F.max("sqft").over(w_recent))
)

valid = (
    enriched_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-06-10").cast("date"))
    .select("*", F.greatest("revenue", "units").alias("greatest_revenue_units"))
)

matched_keys = (
    valid
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.year("sale_date") == 2025)
        .select("store_id", "id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned_store_sales_2 = (
    kept_rows
    .filter(F.col("region").isNotNull())
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "sale_date_month_end",
        "sqft",
        "city",
        "region_filled",
        "resolved_region",
        "running_sum_sqft",
        "running_avg_sqft",
        "running_max_sqft",
        "greatest_revenue_units",
        (F.col("revenue") + F.col("running_avg_sqft")).alias("revenue_plus_running_avg_sqft"),
        F.round("greatest_revenue_units", 0).alias("greatest_revenue_units_rounded"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    cleaned_store_sales_2
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "sale_date_month_end",
        "region_filled",
        "running_sum_sqft",
        "running_avg_sqft",
        "running_max_sqft",
    )
)
