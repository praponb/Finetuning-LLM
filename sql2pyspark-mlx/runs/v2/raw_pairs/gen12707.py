from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2025)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_3"),
    )
    .orderBy(F.col("cost").desc())
)
