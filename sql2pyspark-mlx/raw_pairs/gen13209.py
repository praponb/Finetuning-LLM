from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("shipped_date", "id")

with_windows = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.lag("cost").over(w_recent).alias("prev_cost"),
        F.lead("cost").over(w_recent).alias("next_cost"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "delivered_date",
        "status",
        "prev_cost",
        "next_cost",
        F.when(F.col("status").rlike("^(RETURNED|DELIVERED)$"), "GROUP_A").when(F.col("status").rlike("^(IN_TRANSIT|LOST)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc(), F.col("id").desc())
)
