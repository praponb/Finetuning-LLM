from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id")
w2 = Window.partitionBy(F.month("sale_date"), "store_id")
w_recent = Window.partitionBy("store_id").orderBy("sqft", "id")
w4 = Window.partitionBy("store_id").orderBy("units_whole_filled", "id")
w5 = Window.partitionBy(F.date_trunc("WEEK", "sale_date"), "store_id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-03").cast("date"))
    .select("*", F.ceil("units").alias("units_whole"))
)

joined = (
    cleaned
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .filter(F.col("p.sqft").between(14713, 16066))
    .select(
        "s.id",
        "s.store_id",
        "s.sale_date",
        "s.units",
        "s.revenue",
        "s.units_whole",
        F.col("p.sqft"),
    )
)

with_windows = (
    joined
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w_ordered))
    .withColumn("corr_units_revenue", F.corr("units", "revenue").over(w2))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w_ordered)), 4))
    .withColumn("next_units", F.lead("units", 1, 0).over(w_recent))
)

filtered = (
    with_windows
    .filter((F.col("sale_date") >= F.lit("2023-03-11").cast("date")) & (F.col("revenue") < 2600))
    .withColumn("units_whole_filled", F.coalesce("units_whole", F.lit(0)))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

with_windows_store_sales = (
    filtered
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "corr_revenue_units",
        "corr_units_revenue",
        "sqft_share",
        "units_whole_filled",
        "days_since_sale_date",
        F.lead("corr_units_revenue").over(w4).alias("next_corr_units_revenue"),
        F.lag("days_since_sale_date").over(w4).alias("prev_days_since_sale_date"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("sqft_share").over(w5).alias("sale_date_period_avg_sqft_share"),
    )
)

result = (
    with_windows_store_sales
    .select(
        "id",
        "sale_date",
        "corr_revenue_units",
        "corr_units_revenue",
        "days_since_sale_date",
        "row_seq",
        "sale_date_period_avg_sqft_share",
    )
)
