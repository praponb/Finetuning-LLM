from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-07-14").cast("date"))
    .withColumn("status_upper", F.upper("status"))
    .withColumn("cost_int", F.col("cost").cast("int"))
)

result = (
    base
    .filter(F.col("shipped_date") <= "2025-10-01")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.min("cost_int").over(w).alias("running_min_cost_int"),
        F.sum("cost_int").over(w).alias("running_sum_cost_int"),
        F.avg("cost_int").over(w).alias("running_avg_cost_int"),
    )
)
