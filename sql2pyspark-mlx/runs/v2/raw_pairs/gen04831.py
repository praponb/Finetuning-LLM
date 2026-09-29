from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("order_id").isin([4, 11, 33]))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.lead("cost", 1, 0).over(w).alias("next_cost"),
    )
    .orderBy(F.col("id").desc())
)
