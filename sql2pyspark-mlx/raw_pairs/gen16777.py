from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id", "carrier")
w_running = Window.partitionBy("carrier", "order_id").orderBy("cost", "id")
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("order_id").orderBy("days_since_shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "shipped_date",
        "cost",
        "days_since_shipped_date",
        "carrier_clean",
        F.max("days_since_shipped_date").over(w_hist).alias("order_id_max_days_since_shipped_date"),
        F.ntile(3).over(w_running).alias("cost_bucket"),
        F.last("carrier", ignorenulls=True).over(w3).alias("last_known_carrier"),
        F.first("days_since_shipped_date", ignorenulls=True).over(w_running_2).alias("first_known_days_since_shipped_date"),
        F.first("days_since_shipped_date", ignorenulls=True).over(w5).alias("first_known_days_since_shipped_date_2"),
    )
)
