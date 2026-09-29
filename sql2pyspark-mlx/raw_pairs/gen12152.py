from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("carrier").orderBy("years_since_shipped_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.month("delivered_date"), "order_id").orderBy("last7_sum_years_since_shipped_date", "id")
w3 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("order_id") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        (F.datediff(F.current_date(), "shipped_date") / 365.25).alias("years_since_shipped_date"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

with_windows = (
    valid
    .withColumn("last7_sum_years_since_shipped_date", F.sum("years_since_shipped_date").over(w_grp))
    .withColumn("last7_avg_years_since_shipped_date", F.avg("years_since_shipped_date").over(w_grp))
    .withColumn("last7_max_years_since_shipped_date", F.max("years_since_shipped_date").over(w_grp))
    .withColumn("last7_count_years_since_shipped_date", F.count("years_since_shipped_date").over(w_grp))
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.channel"), F.col("d.discount"), F.col("d.status"))
)

metrics = (
    with_dims
    .filter(F.col("years_since_shipped_date") < 74)
    .select(
        "id",
        "order_id",
        "carrier",
        "last7_avg_years_since_shipped_date",
        "discount",
        F.lead("last7_avg_years_since_shipped_date", 1).over(w2).alias("next_last7_avg_years_since_shipped_date"),
        F.first("carrier", ignorenulls=True).over(w3).alias("first_known_carrier"),
    )
)

with_windows_shipments = (
    metrics
    .withColumn("running_count_next_last7_avg_years_since_shipped_date", F.count("next_last7_avg_years_since_shipped_date").over(w4))
    .withColumn("running_max_next_last7_avg_years_since_shipped_date", F.max("next_last7_avg_years_since_shipped_date").over(w4))
    .withColumn("running_sum_next_last7_avg_years_since_shipped_date", F.sum("next_last7_avg_years_since_shipped_date").over(w4))
)

result = (
    with_windows_shipments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.status").alias("status"), F.col("p.channel").alias("channel"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.running_count_next_last7_avg_years_since_shipped_date")), 2).alias("avg_running_count_next_last7_avg_years_since_shipped_date"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
