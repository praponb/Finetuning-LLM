from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "delivered_date",
        "shipped_date",
        F.first("shipped_date").over(w).alias("first_shipped_date"),
    )
    .orderBy("delivered_date")
)
