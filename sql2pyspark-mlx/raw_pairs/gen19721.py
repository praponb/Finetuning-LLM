from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("weeks_shipped_date_to_delivered_date", "id")
w2 = Window.partitionBy(F.year("shipped_date"), "status").orderBy("cost", "id")
w3 = Window.partitionBy("order_id").orderBy("cost", "id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-04-29"))) & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
        F.ceil("cost").alias("cost_whole"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

result = (
    valid
    .filter(F.col("status") == "RETURNED")
    .select(
        "id",
        "order_id",
        "weeks_shipped_date_to_delivered_date",
        "cost_whole",
        "resolved_carrier",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(10).over(w2).alias("cost_bucket"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
