from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        (F.col("cost") - F.lag("cost", 1).over(w)).alias("cost_change"),
    )
    .orderBy(F.col("cost").desc())
)
