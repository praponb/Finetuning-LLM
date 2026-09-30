from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("sqft", "id")
w_part = Window.orderBy("last3_min_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("last_known_last3_avg_revenue", "id")
w5 = Window.orderBy("sqft_bucket", "id")
w_recent = Window.orderBy(F.col("first_known_sqft_filled").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("revenue") < 2200)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.datediff(F.current_date(), "sale_date") / 30).alias("months_since_sale_date"),
        F.round("units", 0).alias("units_rounded"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.revenue",
        "a.months_since_sale_date",
        "a.units_rounded",
        F.col("d.sqft"),
        F.col("d.store_name"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "revenue",
        "sqft",
        F.min("revenue").over(w_hist).alias("last3_min_revenue"),
        F.avg("revenue").over(w_hist).alias("last3_avg_revenue"),
        F.max("revenue").over(w_hist).alias("last3_max_revenue"),
    )
)

valid_store_sales = (
    with_windows
    .filter(F.col("last3_max_revenue") >= 3200)
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
)

metrics = (
    valid_store_sales
    .select(
        "id",
        "last3_avg_revenue",
        "last3_max_revenue",
        F.ntile(4).over(w2).alias("sqft_bucket"),
        F.last("last3_avg_revenue", ignorenulls=True).over(w_part).alias("last_known_last3_avg_revenue"),
        F.first("sqft_filled", ignorenulls=True).over(w_part).alias("first_known_sqft_filled"),
    )
)

with_windows_store_sales = (
    metrics
    .filter(F.col("last_known_last3_avg_revenue") > 3400)
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.lead("last3_avg_revenue", 2).over(w5).alias("next_last3_avg_revenue"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

result = (
    with_windows_store_sales
    .select(
        "id",
        F.col("last_known_last3_avg_revenue").alias("LAST_KNOWN_LAST3_AVG_REVENUE"),
        "last3_max_revenue",
        F.col("last3_avg_revenue").alias("LAST3_AVG_REVENUE_VAL"),
    )
    .unpivot(
        ids=["id"],
        values=["LAST_KNOWN_LAST3_AVG_REVENUE", "last3_max_revenue", "LAST3_AVG_REVENUE_VAL"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
