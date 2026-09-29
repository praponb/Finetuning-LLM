from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("units_rounded").desc(), "id")
w3 = Window.orderBy("revenue_filled", "id")
w_ordered = Window.orderBy("units_rounded_bucket", "id")
w5 = Window.orderBy(F.col("first_known_revenue_filled_int").desc(), "id")

filtered = (
    df_store_sales
    .filter(F.year("sale_date") >= 2025)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("units").between(33, 36))
    .select(
        "id",
        "units",
        "revenue_per_unit",
        "revenue_filled",
        F.col("revenue_filled").cast("int").alias("revenue_filled_int"),
        F.when(F.col("units") >= 38, "High").when(F.col("units") >= 9, "Medium").otherwise("Low").alias("units_band"),
    )
)

cleaned = (
    valid
    .filter(F.col("units").between(12, 30))
    .withColumn("adjusted_revenue_filled_int", F.round((F.col("revenue_filled_int") * 1.07), 2))
    .withColumn("units_rounded", F.round("units", 1))
)

with_windows = (
    cleaned
    .select(
        "id",
        "revenue_per_unit",
        "revenue_filled_int",
        "units_band",
        "adjusted_revenue_filled_int",
        "units_rounded",
        F.first("revenue_filled_int", ignorenulls=True).over(w).alias("first_known_revenue_filled_int"),
        F.ntile(5).over(w2).alias("units_rounded_bucket"),
        F.ntile(4).over(w3).alias("revenue_filled_bucket"),
    )
)

enriched = (
    with_windows
    .filter(F.col("revenue_per_unit") > 30)
    .select(
        "id",
        "units_band",
        "units_rounded_bucket",
        "revenue_filled_bucket",
        F.lead("revenue_filled_int").over(w_ordered).alias("next_revenue_filled_int"),
        F.lag("adjusted_revenue_filled_int").over(w_ordered).alias("prev_adjusted_revenue_filled_int"),
        F.lead("adjusted_revenue_filled_int").over(w_ordered).alias("next_adjusted_revenue_filled_int"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    enriched
    .cube(F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW").alias("seq_no_tier"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("next_revenue_filled_int"), 2).alias("avg_next_revenue_filled_int"),
        F.min("prev_adjusted_revenue_filled_int").alias("min_prev_adjusted_revenue_filled_int"),
    )
)
