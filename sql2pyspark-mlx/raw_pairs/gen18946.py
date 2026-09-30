from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy(F.col("cost_share").desc(), "id")
w4 = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy(F.col("first_known_cost").asc_nulls_last(), "id")

with_windows = (
    df_shipments
    .filter(F.year("delivered_date") == 2023)
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

with_windows_shipments = (
    with_windows
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "first_known_cost",
        F.ntile(4).over(w3).alias("cost_share_bucket"),
        F.sum("first_known_cost").over(w4).alias("running_first_known_cost"),
    )
)

ranked = (
    with_windows_shipments
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

valid = (
    ranked
    .filter((F.col("shipped_date") > F.lit("2023-08-06").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .withColumn("adjusted_row_num", F.round((F.col("row_num") * 1.1), 2))
)

daily = (
    valid
    .groupBy(F.date_trunc("quarter", "shipped_date").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("adjusted_row_num")).otherwise(0)).alias("delivered_adjusted_row_num"),
    )
)

result = daily.select("quarter_start", "status_list", "delivered_adjusted_row_num")
