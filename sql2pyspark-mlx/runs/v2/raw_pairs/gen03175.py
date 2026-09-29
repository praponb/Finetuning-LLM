from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "order_id",
        "shipped_date",
        F.count("*").over(w).alias("carrier_count"),
        F.row_number().over(w2).alias("row_num"),
    )
)
