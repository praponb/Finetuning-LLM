from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "delivered_date",
        "carrier",
        F.first("carrier").over(w).alias("first_carrier"),
    )
)
