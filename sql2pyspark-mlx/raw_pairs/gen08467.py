from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.lag("cost").over(w_grp).alias("prev_cost"),
        F.lead("cost").over(w_grp).alias("next_cost"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
