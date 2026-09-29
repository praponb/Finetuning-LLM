from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("next_revenue", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy(F.col("next_revenue").desc(), "id")
w_ordered = Window.orderBy("seq_no", "id")
w_part = Window.orderBy("last7_max_days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.lead("revenue", 1, 0).over(w).alias("next_revenue"),
        F.last("units", ignorenulls=True).over(w2).alias("last_known_units"),
    )
)

valid = (
    metrics
    .filter(F.col("units") != 30)
    .select(
        "id",
        "sale_date",
        "next_revenue",
        "last_known_units",
        F.round((F.col("units") * 1.07), 2).alias("units_adjusted"),
        F.round((F.col("next_revenue") / F.col("units")), 2).alias("next_revenue_per_units"),
    )
)

valid_store_sales = (
    valid
    .filter(F.col("sale_date") > F.lit("2023-08-07").cast("date"))
    .select(
        "id",
        "next_revenue",
        "units_adjusted",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

scored = (
    valid_store_sales
    .select(
        "*",
        F.sum("days_since_sale_date").over(w3).alias("last7_sum_days_since_sale_date"),
        F.avg("days_since_sale_date").over(w3).alias("last7_avg_days_since_sale_date"),
        F.max("days_since_sale_date").over(w3).alias("last7_max_days_since_sale_date"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

metrics_store_sales = (
    scored
    .select(
        "id",
        "next_revenue",
        "units_adjusted",
        "days_since_sale_date",
        "last7_avg_days_since_sale_date",
        F.lead("seq_no").over(w_ordered).alias("next_seq_no"),
        F.lag("seq_no").over(w_ordered).alias("prev_seq_no"),
        F.lag("next_revenue").over(w_ordered).alias("prev_next_revenue"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.first("days_since_sale_date", ignorenulls=True).over(w_part).alias("first_known_days_since_sale_date"),
    )
)

report = (
    metrics_store_sales
    .select(
        "id",
        "next_revenue",
        "days_since_sale_date",
        "next_seq_no",
        "prev_seq_no",
        "prev_next_revenue",
        "row_seq",
        "first_known_days_since_sale_date",
    )
)

result = (
    report
    .select("id", "prev_next_revenue", "row_seq", "first_known_days_since_sale_date")
    .orderBy(F.col("prev_next_revenue").asc_nulls_last())
)
