from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-08").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)

scored = (
    base
    .withColumn("next_units", F.lead("units").over(w_hist))
    .withColumn("prev_revenue_per_unit", F.lag("revenue_per_unit").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "sale_date",
        "revenue",
        "revenue_per_unit",
        "prev_revenue_per_unit",
        "event_seq",
    )
)

base_store_sales = (
    latest
    .filter(F.col("sale_date") > F.lit("2023-04-09").cast("date"))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
    .withColumn("event_seq_rounded", F.round("event_seq", 1))
)

result = (
    base_store_sales
    .select(
        "id",
        "sale_date",
        "revenue_per_unit",
        "prev_revenue_per_unit",
        "sale_date_month_end",
        "event_seq_rounded",
    )
    .orderBy(F.col("sale_date_month_end").asc_nulls_last(), "id")
)
