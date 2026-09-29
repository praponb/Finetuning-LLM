from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(-2, -1)
w_recent = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("delivered_date"), "order_id")
w4 = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier")
w6 = Window.orderBy("amount_avg_2_prior", "id").rowsBetween(-6, -1)
w7 = Window.orderBy("delivered_date_year_max_cost", "id")

with_dims = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.amount"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "shipped_date",
        F.avg("amount").over(w).alias("amount_avg_2_prior"),
        F.sum("cost").over(w_recent).alias("cumulative_cost"),
        F.max("cost").over(w3).alias("delivered_date_year_max_cost"),
        F.last("carrier", ignorenulls=True).over(w4).alias("last_known_carrier"),
        F.max("cost").over(w5).alias("carrier_max_cost"),
    )
)

with_windows_shipments = (
    with_windows
    .select(
        "id",
        "cumulative_cost",
        "delivered_date_year_max_cost",
        "last_known_carrier",
        F.avg("carrier_max_cost").over(w6).alias("carrier_max_cost_avg_6_prior"),
        F.ntile(3).over(w7).alias("delivered_date_year_max_cost_bucket"),
    )
)

cleaned = (
    with_windows_shipments
    .filter(F.col("delivered_date_year_max_cost") < 48)
    .select(
        "id",
        "cumulative_cost",
        "delivered_date_year_max_cost",
        "delivered_date_year_max_cost_bucket",
        F.round("carrier_max_cost_avg_6_prior", 1).alias("carrier_max_cost_avg_6_prior_rounded"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "delivered_date_year_max_cost_bucket",
        "carrier_max_cost_avg_6_prior_rounded",
    )
    .orderBy(F.col("carrier_max_cost_avg_6_prior_rounded").desc_nulls_last())
)
