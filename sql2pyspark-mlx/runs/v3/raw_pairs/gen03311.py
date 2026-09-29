from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
)
