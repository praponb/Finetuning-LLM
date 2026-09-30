from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w_running = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("units").desc(), "id")

metrics = (
    df_store_sales
    .filter(F.year("sale_date") >= 2024)
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.first("revenue", ignorenulls=True).over(w_running).alias("first_known_revenue"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("activity_month"))
    .agg(
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.bit_and("units").alias("units_bit_and"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("row_seq_p90") > 0)
)
