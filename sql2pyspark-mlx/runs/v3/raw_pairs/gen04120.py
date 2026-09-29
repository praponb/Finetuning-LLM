from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.first("cost").over(w).alias("first_cost"),
    )
)
