from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "product_id").orderBy("units", "id")

with_windows = (
    df_store_sales
    .filter(F.col("units") >= 26)
    .select(
        "*",
        F.lag("units").over(w_recent).alias("prev_units"),
        F.lead("units").over(w_recent).alias("next_units"),
        F.lag("revenue").over(w_recent).alias("prev_revenue"),
        F.lead("revenue").over(w_recent).alias("next_revenue"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("week", "sale_date").alias("week_start"), "product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("prev_units", 0.95).alias("prev_units_p95"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
    .filter(F.col("prev_units_p95") > 0)
    .orderBy(F.col("cnt").desc())
)
