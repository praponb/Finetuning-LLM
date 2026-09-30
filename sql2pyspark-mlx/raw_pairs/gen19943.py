from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w3 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("order_id", "carrier").orderBy("months_shipped_date_to_delivered_date")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= "2025-06-29") & (F.col("shipped_date") > F.lit("2023-03-19").cast("date")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.round((F.datediff("delivered_date", "shipped_date") / 30), 1).alias("months_shipped_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "carrier",
        "cost",
        "months_shipped_date_to_delivered_date",
        "carrier_filled",
        F.lead("months_shipped_date_to_delivered_date", 2).over(w).alias("next_months_shipped_date_to_delivered_date"),
        F.lead("months_shipped_date_to_delivered_date", 1, 0).over(w2).alias("next_months_shipped_date_to_delivered_date_2"),
        F.avg("cost").over(w3).alias("cost_avg_6_prior"),
        F.percent_rank().over(w4).alias("months_shipped_date_to_delivered_date_percent_rank"),
    )
)
