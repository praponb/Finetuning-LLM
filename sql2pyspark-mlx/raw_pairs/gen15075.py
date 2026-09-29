from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "shipped_date"), "order_id")
w2 = Window.partitionBy("status", "carrier").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "order_id",
        "status",
        "carrier",
        F.count("*").over(w).alias("shipped_date_period_count"),
        F.row_number().over(w2).alias("row_num"),
    )
)
