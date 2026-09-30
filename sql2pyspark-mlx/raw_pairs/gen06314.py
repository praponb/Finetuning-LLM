from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first())

valid = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("DHL"))) & F.col("cost").between(44, 88))
    .select("*", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)

result = (
    valid
    .filter(F.col("shipped_date") >= "2023-11-01")
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_filled",
        F.count("cost").over(w_hist).alias("running_count_cost"),
        F.sum("cost").over(w_hist).alias("running_sum_cost"),
        F.min("cost").over(w_hist).alias("running_min_cost"),
        F.avg("cost").over(w_hist).alias("running_avg_cost"),
        F.percent_rank().over(w_ordered).alias("cost_percent_rank"),
    )
)
