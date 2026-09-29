from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "status",
        "cost",
        F.sum("cost").over(w).alias("carrier_sum_cost"),
    )
    .orderBy(F.col("status").desc())
)
