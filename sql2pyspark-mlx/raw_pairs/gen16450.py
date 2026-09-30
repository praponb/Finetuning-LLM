from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("shipped_date"), "status").orderBy("cost", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy(F.date_trunc("YEAR", "shipped_date"), "order_id")

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-08-22")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        F.sum("cost").over(w).alias("running_cost"),
        F.avg("cost").over(w2).alias("order_id_avg_cost"),
        F.sum("cost").over(w3).alias("cumulative_cost"),
        F.avg("cost").over(w4).alias("cost_avg_6_prior"),
        F.stddev_samp("cost").over(w5).alias("shipped_date_period_stddev_samp_cost"),
    )
)
