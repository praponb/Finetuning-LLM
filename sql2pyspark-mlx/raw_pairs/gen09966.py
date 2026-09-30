from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last())
w_hist = Window.partitionBy(F.year("shipped_date_month_end"), "order_id")

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.lower("carrier").alias("carrier_lower"),
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

result = (
    valid
    .filter(F.col("delivered_date").between("2023-05-01", "2024-12-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        F.sum("cost").over(w_grp).alias("running_cost"),
        F.percent_rank().over(w2).alias("cost_percent_rank"),
        F.max("cost").over(w_hist).alias("shipped_date_month_end_year_max_cost"),
    )
)
