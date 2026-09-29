from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status", "carrier").orderBy("cost", "id")

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        F.lag("cost").over(w_recent).alias("prev_cost"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)
